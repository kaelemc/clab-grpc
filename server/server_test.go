package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	clabv1 "github.com/kaelemc/clab-grpc/gen/clabv1"

	clabconstants "github.com/srl-labs/containerlab/constants"
	clabruntime "github.com/srl-labs/containerlab/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTimeout(t *testing.T) {
	if got := timeout(0); got != defaultTimeout {
		t.Errorf("timeout(0) = %v, want default %v", got, defaultTimeout)
	}
	if got := timeout(5); got != 5*time.Second {
		t.Errorf("timeout(5) = %v, want 5s", got)
	}
}

func TestNodeName(t *testing.T) {
	// label wins
	c := &clabruntime.GenericContainer{
		Names:  []string{"/clab-lab1-r1"},
		Labels: map[string]string{clabconstants.NodeName: "r1"},
	}
	if got := nodeName(c); got != "r1" {
		t.Errorf("nodeName label = %q, want r1", got)
	}
	// fall back to trimmed container name
	c2 := &clabruntime.GenericContainer{Names: []string{"/clab-lab1-r2"}}
	if got := nodeName(c2); got != "clab-lab1-r2" {
		t.Errorf("nodeName fallback = %q, want clab-lab1-r2", got)
	}
}

func TestToNode(t *testing.T) {
	c := &clabruntime.GenericContainer{
		Names:  []string{"/clab-lab1-r1"},
		Image:  "alpine:3",
		State:  "running",
		Labels: map[string]string{clabconstants.NodeName: "r1", clabconstants.NodeKind: "linux"},
		NetworkSettings: clabruntime.GenericMgmtIPs{
			IPv4addr: "172.20.20.2", IPv4pLen: 24,
		},
	}
	n := toNode(c)
	if n.Name != "r1" || n.Kind != "linux" || n.Image != "alpine:3" || n.State != "running" {
		t.Errorf("toNode basic fields wrong: %+v", n)
	}
	if n.Ipv4Address != "172.20.20.2/24" {
		t.Errorf("toNode ipv4 = %q, want 172.20.20.2/24", n.Ipv4Address)
	}
}

func TestToStatus(t *testing.T) {
	if toStatus(nil) != nil {
		t.Error("toStatus(nil) should be nil")
	}
	if code := status.Code(toStatus(context.DeadlineExceeded)); code != codes.DeadlineExceeded {
		t.Errorf("deadline -> %v, want DeadlineExceeded", code)
	}
	if code := status.Code(toStatus(errString("boom"))); code != codes.Internal {
		t.Errorf("generic -> %v, want Internal", code)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestDeployRequiresTopology(t *testing.T) {
	_, err := (&server{}).Deploy(context.Background(), &clabv1.DeployRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty deploy -> %v, want InvalidArgument", err)
	}
	_, err = (&server{}).Redeploy(context.Background(), &clabv1.RedeployRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty redeploy -> %v, want InvalidArgument", err)
	}
	_, err = (&server{}).Destroy(context.Background(), &clabv1.DestroyRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty destroy -> %v, want InvalidArgument", err)
	}
	_, err = (&server{}).Exec(context.Background(), &clabv1.ExecRequest{Commands: []string{"true"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("exec without lab_name -> %v, want InvalidArgument", err)
	}
}

func TestLabNameFromYAML(t *testing.T) {
	if got, err := labNameFromYAML([]byte("name: t1\n")); err != nil || got != "t1" {
		t.Errorf("labNameFromYAML = %q, %v; want t1", got, err)
	}
	if _, err := labNameFromYAML([]byte("nodes: {}\n")); err == nil {
		t.Error("missing name should error")
	}
	for _, bad := range []string{"a/b", "..", "."} {
		if _, err := labNameFromYAML([]byte("name: " + bad + "\n")); err == nil {
			t.Errorf("name %q should be rejected", bad)
		}
	}
}

func TestWriteTopo(t *testing.T) {
	s := &server{baseDir: t.TempDir()}
	p, err := s.writeTopo("t1", []byte("name: t1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(s.baseDir, "t1", topoFileName); p != want {
		t.Errorf("topo path = %q, want %q", p, want)
	}
	b, err := os.ReadFile(p)
	if err != nil || string(b) != "name: t1\n" {
		t.Errorf("read back %q, %v; want file content", b, err)
	}
}
