package main

import (
	"flag"
	"log"
	"os"

	"github.com/kaelemc/clab-grpc/server"
)

func main() {
	port := flag.Int("port", 8091, "TCP port to listen on")
	baseDir := flag.String("base-dir", envOr("CLAB_GRPC_BASE_DIR", server.DefaultBaseDir),
		"base directory for persistent per-lab working directories (must not be under /tmp)")
	flag.Parse()

	if err := server.Serve(*port, *baseDir); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
