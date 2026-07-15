package main

import (
	"context"
	"flag"
	"fmt"
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
	port := flag.Int("port", 8091, "TCP port to listen on")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
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
