package main

import (
	"context"
	"time"

	clabv1 "clabgrpc/gen/clabv1"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

const defaultStatsInterval = 2 * time.Second

// sampleHostStats measures per-core CPU over `window` (cpu.Percent blocks for
// it) and reads current memory. Core counts and load average are best-effort:
// a failure enriching those never fails the whole sample.
func sampleHostStats(ctx context.Context, window time.Duration) (*clabv1.HostStats, error) {
	// one per-core measurement; the aggregate is the mean, so we don't pay a
	// second blocking window for the total.
	perCPU, err := cpu.PercentWithContext(ctx, window, true)
	if err != nil {
		return nil, err
	}
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, err
	}

	st := &clabv1.HostStats{
		PerCpuPercent: perCPU,
		MemTotalBytes: vm.Total,
		MemUsedBytes:  vm.Used,
		MemPercent:    vm.UsedPercent,
	}
	if len(perCPU) > 0 {
		var sum float64
		for _, p := range perCPU {
			sum += p
		}
		st.CpuPercent = sum / float64(len(perCPU))
	}

	if n, err := cpu.CountsWithContext(ctx, true); err == nil {
		st.CpuCount = uint32(n)
	}
	if n, err := cpu.CountsWithContext(ctx, false); err == nil {
		st.PhysicalCpuCount = uint32(n)
	}
	if la, err := load.AvgWithContext(ctx); err == nil {
		st.Load1, st.Load5, st.Load15 = la.Load1, la.Load5, la.Load15
	}
	return st, nil
}

func (s *server) GetHostStats(ctx context.Context, _ *clabv1.HostStatsRequest) (*clabv1.HostStats, error) {
	st, err := sampleHostStats(ctx, 200*time.Millisecond)
	if err != nil {
		return nil, toStatus(err)
	}
	return st, nil
}

func (s *server) StreamHostStats(req *clabv1.StreamHostStatsRequest, stream clabv1.Containerlab_StreamHostStatsServer) error {
	interval := time.Duration(req.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = defaultStatsInterval
	}
	ctx := stream.Context()
	for {
		// cpu.Percent blocks for `interval`, so it both measures and paces the stream.
		st, err := sampleHostStats(ctx, interval)
		if err != nil {
			if ctx.Err() != nil {
				// client disconnected
				return nil
			}
			return toStatus(err)
		}
		if err := stream.Send(st); err != nil {
			return err
		}
	}
}
