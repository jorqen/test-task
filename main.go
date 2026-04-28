package main

import (
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"

	"test-task/internal/broker"
)

func main() {
	var port uint
	flag.UintVar(&port, "port", 0, "port to listen on")
	flag.Parse()

	if port == 0 || port > math.MaxUint16 {
		slog.Error("port is required and must be between 1 and 65535", "flag", "-port")
		os.Exit(2)
	}

	addr := fmt.Sprintf(":%d", port)
	if err := http.ListenAndServe(addr, broker.New()); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
