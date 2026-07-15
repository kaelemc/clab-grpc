package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	clabv1 "clabgrpc/gen/clabv1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func main() {
	sock := flag.String("socket", "/run/clab-grpc.sock", "unix socket to listen on")
	addr := flag.String("listen", "", "optional TCP address (e.g. :5555); overrides -socket")
	flag.Parse()

	lis, err := listen(*addr, *sock)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	gs := grpc.NewServer(grpc.UnaryInterceptor(recoverInterceptor))
	clabv1.RegisterContainerlabServer(gs, &server{})
	// lets grpcurl work without local proto files
	reflection.Register(gs)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down")
		gs.GracefulStop()
	}()

	log.Printf("clab-grpc listening on %s", lis.Addr())
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func listen(addr, sock string) (net.Listener, error) {
	if addr != "" {
		return net.Listen("tcp", addr)
	}
	// clear a stale socket from a previous run
	_ = os.Remove(sock)
	return net.Listen("unix", sock)
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
