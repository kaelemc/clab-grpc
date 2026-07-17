package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	clabv1 "github.com/kaelemc/clab-grpc/gen/clabv1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Serve serves the Containerlab gRPC service on port until SIGINT/SIGTERM,
// then stops gracefully.
func Serve(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	gs := grpc.NewServer(
		grpc.UnaryInterceptor(recoverInterceptor),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             1 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	srv := &server{stop: make(chan struct{})}
	clabv1.RegisterContainerlabServer(gs, srv)
	// lets grpcurl work without local proto files
	reflection.Register(gs)

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down")
		close(srv.stop)
		hs.Shutdown()
		gs.GracefulStop()
	}()

	log.Printf("clab-grpc listening on %s", lis.Addr())
	return gs.Serve(lis)
}

// recoverInterceptor keeps a panic in containerlab core from killing the daemon.
func recoverInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in handler: %v", r)
			err = status.Errorf(codes.Internal, "internal panic: %v", r)
		}
	}()
	return handler(ctx, req)
}
