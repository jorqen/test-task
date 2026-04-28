package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"test-task/types"
)

func main() {
	var port uint
	flag.UintVar(&port, "port", 8080, "port to listen on")
	flag.Parse()

	addr := fmt.Sprintf(":%d", port)
	log.Fatal(http.ListenAndServe(addr, types.NewBroker()))
}
