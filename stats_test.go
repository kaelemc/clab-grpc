package main

import (
	"context"
	"testing"

	clabv1 "clabgrpc/gen/clabv1"
)

func TestGetHostStats(t *testing.T) {
	st, err := (&server{}).GetHostStats(context.Background(), &clabv1.HostStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.MemTotalBytes == 0 || st.MemUsedBytes == 0 {
		t.Errorf("memory looks unset: %+v", st)
	}
	if st.CpuPercent < 0 || st.CpuPercent > 100 {
		t.Errorf("cpu_percent = %v, want 0-100", st.CpuPercent)
	}
	if st.MemPercent <= 0 || st.MemPercent > 100 {
		t.Errorf("mem_percent = %v, want 0-100", st.MemPercent)
	}
	if st.CpuCount == 0 {
		t.Error("cpu_count should be > 0")
	}
	if len(st.PerCpuPercent) != int(st.CpuCount) {
		t.Errorf("per_cpu_percent len = %d, want cpu_count %d", len(st.PerCpuPercent), st.CpuCount)
	}
	if st.Load1 < 0 {
		t.Errorf("load1 = %v, want >= 0", st.Load1)
	}
}
