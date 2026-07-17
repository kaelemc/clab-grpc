package server

import (
	"context"
	"testing"
	"time"

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
	if code := status.Code(toStatus(errString("open x.yml: no such file or directory"))); code != codes.InvalidArgument {
		t.Errorf("missing file -> %v, want InvalidArgument", code)
	}
	if code := status.Code(toStatus(errString("boom"))); code != codes.Internal {
		t.Errorf("generic -> %v, want Internal", code)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
