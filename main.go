package main

import (
	"flag"
	"log"

	"github.com/kaelemc/clab-grpc/server"
)

func main() {
	port := flag.Int("port", 8091, "TCP port to listen on")
	flag.Parse()

	if err := server.Serve(*port); err != nil {
		log.Fatal(err)
	}
}
