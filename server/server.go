package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	clabv1 "github.com/kaelemc/clab-grpc/gen/clabv1"

	clabconstants "github.com/srl-labs/containerlab/constants"
	clabcore "github.com/srl-labs/containerlab/core"
	clabexec "github.com/srl-labs/containerlab/exec"
	clabruntime "github.com/srl-labs/containerlab/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultTimeout = 120 * time.Second

// server implements clabv1.ContainerlabServer over the containerlab core library.
type server struct {
	clabv1.UnimplementedContainerlabServer
	mu sync.Mutex // serializes mutating RPCs
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

// toStatus maps a core error to a gRPC status.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if strings.Contains(err.Error(), "no such file") {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
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
	if req.TopologyPath == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_path is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doDeploy(ctx, req)
}

// doDeploy deploys without locking so Redeploy can reuse it.
func (s *server) doDeploy(ctx context.Context, req *clabv1.DeployRequest) (*clabv1.LabState, error) {
	opts := commonOpts(req.Runtime, req.TimeoutSeconds, clabcore.WithTopoPath(req.TopologyPath, nil))
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

	containers, err := c.Deploy(ctx, do)
	if err != nil {
		return nil, toStatus(err)
	}
	return labState(c.Config.Name, containers), nil
}

func (s *server) Destroy(ctx context.Context, req *clabv1.DestroyRequest) (*clabv1.DestroyResponse, error) {
	if req.TopologyPath == "" && req.LabName == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_path or lab_name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name, err := s.doDestroy(ctx, req)
	if err != nil {
		return nil, err
	}
	return &clabv1.DestroyResponse{LabName: name}, nil
}

// doDestroy destroys without locking so Redeploy can reuse it.
func (s *server) doDestroy(ctx context.Context, req *clabv1.DestroyRequest) (string, error) {
	target := clabcore.WithLabNameOnly(req.LabName)
	if req.TopologyPath != "" {
		target = clabcore.WithTopoPath(req.TopologyPath, nil)
	}
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
	if req.TopologyPath == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_path is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.doDestroy(ctx, &clabv1.DestroyRequest{
		TopologyPath:   req.TopologyPath,
		Runtime:        req.Runtime,
		TimeoutSeconds: req.TimeoutSeconds,
		Cleanup:        req.Cleanup,
		KeepMgmtNet:    req.KeepMgmtNet,
	}); err != nil {
		return nil, err
	}
	return s.doDeploy(ctx, &clabv1.DeployRequest{
		TopologyPath:   req.TopologyPath,
		Runtime:        req.Runtime,
		TimeoutSeconds: req.TimeoutSeconds,
		MaxWorkers:     req.MaxWorkers,
	})
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
	return labState(req.LabName, containers), nil
}

func (s *server) Exec(ctx context.Context, req *clabv1.ExecRequest) (*clabv1.ExecResponse, error) {
	if req.TopologyPath == "" && req.LabName == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_path or lab_name is required")
	}
	if len(req.Commands) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commands is required")
	}

	target := clabcore.WithLabNameOnly(req.LabName)
	if req.TopologyPath != "" {
		target = clabcore.WithTopoPath(req.TopologyPath, nil)
	}
	c, err := clabcore.NewContainerLab(commonOpts(req.Runtime, req.TimeoutSeconds, target)...)
	if err != nil {
		return nil, toStatus(err)
	}

	labName := req.LabName
	if labName == "" {
		labName = c.Config.Name
	}
	containers, err := c.ListContainers(ctx, clabcore.WithListLabName(labName))
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
