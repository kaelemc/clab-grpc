package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	clabv1 "github.com/kaelemc/clab-grpc/gen/clabv1"

	clabconstants "github.com/srl-labs/containerlab/constants"
	clabcore "github.com/srl-labs/containerlab/core"
	clabexec "github.com/srl-labs/containerlab/exec"
	clablinks "github.com/srl-labs/containerlab/links"
	clabruntime "github.com/srl-labs/containerlab/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	yaml "gopkg.in/yaml.v3"
)

const defaultTimeout = 120 * time.Second

// DefaultBaseDir holds persistent per-lab working dirs. Outside /tmp so tmp
// cleaners can't delete running nodes' bind-mount sources.
const DefaultBaseDir = "/var/lib/clab-grpc"

const topoFileName = "topology.clab.yml"

// server implements clabv1.ContainerlabServer over the containerlab core library.
type server struct {
	clabv1.UnimplementedContainerlabServer
	stop    chan struct{} // closed on shutdown; ends open streams so GracefulStop can finish
	mu      sync.Mutex
	baseDir string
}

func timeout(sec uint32) time.Duration {
	if sec == 0 {
		return defaultTimeout
	}
	return time.Duration(sec) * time.Second
}

func nodeName(c *clabruntime.GenericContainer) string {
	if n := c.Labels[clabconstants.NodeName]; n != "" {
		return n
	}
	if len(c.Names) > 0 {
		return strings.TrimPrefix(c.Names[0], "/")
	}
	return ""
}

func toNode(c *clabruntime.GenericContainer) *clabv1.Node {
	return &clabv1.Node{
		Name:        nodeName(c),
		Kind:        c.Labels[clabconstants.NodeKind],
		Image:       c.Image,
		State:       c.State,
		Ipv4Address: c.GetContainerIPv4(),
		Ipv6Address: c.GetContainerIPv6(),
	}
}

func labState(name string, containers []clabruntime.GenericContainer) *clabv1.LabState {
	ls := &clabv1.LabState{LabName: name}
	for i := range containers {
		ls.Nodes = append(ls.Nodes, toNode(&containers[i]))
	}
	return ls
}

func inspectLabState(name string, containers []clabruntime.GenericContainer) *clabv1.LabState {
	ls := labState(name, containers)
	for i := range containers {
		ls.Nodes[i].Labels = containers[i].Labels
	}
	return ls
}

func labNameFromYAML(y []byte) (string, error) {
	var t struct {
		Name string `yaml:"name"`
	}
	if err := yaml.Unmarshal(y, &t); err != nil {
		return "", fmt.Errorf("parsing topology yaml: %w", err)
	}
	name := strings.TrimSpace(t.Name)
	if name == "" {
		return "", errors.New("topology name is required")
	}
	// The name is joined onto baseDir; reject anything that isn't a lone path element.
	if name != filepath.Base(name) || name == "." || name == ".." {
		return "", fmt.Errorf("invalid topology name %q", name)
	}
	return name, nil
}

// labWorkdir returns the persistent working directory for a lab.
func (s *server) labWorkdir(name string) string {
	base := s.baseDir
	if base == "" {
		base = DefaultBaseDir
	}
	return filepath.Join(base, name)
}

// writeTopo writes the topology into the lab's persistent working dir and returns its path.
func (s *server) writeTopo(name string, y []byte) (string, error) {
	dir := s.labWorkdir(name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(dir, topoFileName)
	if err := os.WriteFile(p, y, 0o644); err != nil {
		return "", err
	}
	return p, nil
}

// toStatus maps a core error to a gRPC status.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func resetImplicitLinkNodes() {
	for _, n := range []clablinks.Node{clablinks.GetHostLinkNode(), clablinks.GetMgmtBrLinkNode()} {
		owner, ok := n.(clablinks.EndpointOwner)
		if !ok {
			continue
		}
		for _, e := range append([]clablinks.Endpoint(nil), n.GetEndpoints()...) {
			_ = owner.ReleaseEndpoint(e)
		}
	}
}

// commonOpts builds the CLab options shared by every RPC, plus any caller opts.
func commonOpts(runtime string, sec uint32, opts ...clabcore.ClabOption) []clabcore.ClabOption {
	d := timeout(sec)
	base := []clabcore.ClabOption{
		clabcore.WithTimeout(d),
		clabcore.WithRuntime(runtime, &clabruntime.RuntimeConfig{Timeout: d}),
	}
	return append(base, opts...)
}

func (s *server) Deploy(ctx context.Context, req *clabv1.DeployRequest) (*clabv1.LabState, error) {
	if len(req.TopologyYaml) == 0 {
		return nil, status.Error(codes.InvalidArgument, "topology_yaml is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name, err := labNameFromYAML(req.TopologyYaml)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	topoPath, err := s.writeTopo(name, req.TopologyYaml)
	if err != nil {
		return nil, toStatus(err)
	}
	// clab-<name>/ next to topoPath holds nodes' bind-mounts for the lab's lifetime; keep it.
	return s.doDeploy(ctx, req, topoPath)
}

// doDeploy deploys from topoPath without locking so Redeploy can reuse it.
func (s *server) doDeploy(ctx context.Context, req *clabv1.DeployRequest, topoPath string) (*clabv1.LabState, error) {
	opts := commonOpts(req.Runtime, req.TimeoutSeconds, clabcore.WithTopoPath(topoPath, nil))
	if len(req.NodeFilter) > 0 {
		opts = append(opts, clabcore.WithNodeFilter(req.NodeFilter))
	}
	c, err := clabcore.NewContainerLab(opts...)
	if err != nil {
		return nil, toStatus(err)
	}
	do, err := clabcore.NewDeployOptions(uint(req.MaxWorkers))
	if err != nil {
		return nil, toStatus(err)
	}
	do.SetReconfigure(req.Reconfigure).SetSkipPostDeploy(req.SkipPostDeploy)

	resetImplicitLinkNodes()
	containers, err := c.Deploy(ctx, do)
	if err != nil {
		return nil, toStatus(err)
	}
	return labState(c.Config.Name, containers), nil
}

func (s *server) Destroy(ctx context.Context, req *clabv1.DestroyRequest) (*clabv1.DestroyResponse, error) {
	if req.LabName == "" {
		return nil, status.Error(codes.InvalidArgument, "lab_name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Use the deploy-time topo path so cleanup targets the exact lab dir; fall
	// back to lab-name resolution for labs deployed before it existed.
	dir := s.labWorkdir(req.LabName)
	topoPath := filepath.Join(dir, topoFileName)
	target := clabcore.WithLabNameOnly(req.LabName)
	if _, statErr := os.Stat(topoPath); statErr == nil {
		target = clabcore.WithTopoPath(topoPath, nil)
	}

	name, err := s.doDestroy(ctx, req, target)
	if err != nil {
		return nil, err
	}
	// Remove the lab dir after teardown so redeploys start clean.
	if err := os.RemoveAll(dir); err != nil {
		return nil, toStatus(err)
	}
	return &clabv1.DestroyResponse{LabName: name}, nil
}

// doDestroy destroys the lab selected by target without locking so Redeploy can reuse it.
func (s *server) doDestroy(ctx context.Context, req *clabv1.DestroyRequest, target clabcore.ClabOption) (string, error) {
	c, err := clabcore.NewContainerLab(
		commonOpts(req.Runtime, req.TimeoutSeconds, target, clabcore.WithSkippedBindsPathsCheck())...,
	)
	if err != nil {
		return "", toStatus(err)
	}
	var dopts []clabcore.DestroyOption
	if req.Cleanup {
		dopts = append(dopts, clabcore.WithDestroyCleanup())
	}
	if req.Graceful {
		dopts = append(dopts, clabcore.WithDestroyGraceful())
	}
	if req.KeepMgmtNet {
		dopts = append(dopts, clabcore.WithDestroyKeepMgmtNet())
	}
	if len(req.NodeFilter) > 0 {
		dopts = append(dopts, clabcore.WithDestroyNodeFilter(req.NodeFilter))
	}
	resetImplicitLinkNodes()
	if err := c.Destroy(ctx, dopts...); err != nil {
		return "", toStatus(err)
	}
	name := req.LabName
	if name == "" {
		name = c.Config.Name
	}
	return name, nil
}

func (s *server) Redeploy(ctx context.Context, req *clabv1.RedeployRequest) (*clabv1.LabState, error) {
	if len(req.TopologyYaml) == 0 {
		return nil, status.Error(codes.InvalidArgument, "topology_yaml is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name, err := labNameFromYAML(req.TopologyYaml)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	topoPath, err := s.writeTopo(name, req.TopologyYaml)
	if err != nil {
		return nil, toStatus(err)
	}
	if _, err := s.doDestroy(ctx, &clabv1.DestroyRequest{
		Runtime:        req.Runtime,
		TimeoutSeconds: req.TimeoutSeconds,
		Cleanup:        req.Cleanup,
		KeepMgmtNet:    req.KeepMgmtNet,
	}, clabcore.WithTopoPath(topoPath, nil)); err != nil {
		return nil, err
	}
	return s.doDeploy(ctx, &clabv1.DeployRequest{
		Runtime:        req.Runtime,
		TimeoutSeconds: req.TimeoutSeconds,
		MaxWorkers:     req.MaxWorkers,
	}, topoPath)
}

func (s *server) Inspect(ctx context.Context, req *clabv1.InspectRequest) (*clabv1.LabState, error) {
	opts := commonOpts(req.Runtime, req.TimeoutSeconds)
	if req.LabName != "" {
		opts = append(opts, clabcore.WithLabNameOnly(req.LabName))
	}
	c, err := clabcore.NewContainerLab(opts...)
	if err != nil {
		return nil, toStatus(err)
	}

	listOpt := clabcore.WithListclabLabelExists()
	if req.LabName != "" {
		listOpt = clabcore.WithListLabName(req.LabName)
	}
	containers, err := c.ListContainers(ctx, listOpt)
	if err != nil {
		return nil, toStatus(err)
	}
	return inspectLabState(req.LabName, containers), nil
}

func (s *server) Exec(ctx context.Context, req *clabv1.ExecRequest) (*clabv1.ExecResponse, error) {
	if req.LabName == "" {
		return nil, status.Error(codes.InvalidArgument, "lab_name is required")
	}
	if len(req.Commands) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commands is required")
	}

	c, err := clabcore.NewContainerLab(
		commonOpts(req.Runtime, req.TimeoutSeconds, clabcore.WithLabNameOnly(req.LabName))...,
	)
	if err != nil {
		return nil, toStatus(err)
	}

	containers, err := c.ListContainers(ctx, clabcore.WithListLabName(req.LabName))
	if err != nil {
		return nil, toStatus(err)
	}

	want := map[string]bool{}
	for _, n := range req.NodeFilter {
		want[n] = true
	}

	// RunExec per container returns typed results, unlike core.Exec's JSON dump.
	resp := &clabv1.ExecResponse{}
	for i := range containers {
		ctr := &containers[i]
		if len(want) > 0 && !want[nodeName(ctr)] {
			continue
		}
		for _, cmdStr := range req.Commands {
			ec, err := clabexec.NewExecCmdFromString(cmdStr)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "bad command %q: %v", cmdStr, err)
			}
			res, err := ctr.RunExec(ctx, ec)
			if err != nil {
				if errors.Is(err, clabexec.ErrRunExecNotSupported) {
					continue
				}
				return nil, toStatus(err)
			}
			resp.Results = append(resp.Results, &clabv1.ExecResult{
				Node:       nodeName(ctr),
				Cmd:        res.Cmd,
				ReturnCode: int32(res.ReturnCode),
				Stdout:     []byte(res.Stdout),
				Stderr:     []byte(res.Stderr),
			})
		}
	}
	return resp, nil
}
