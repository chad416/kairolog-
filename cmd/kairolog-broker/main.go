package main

import (
	"fmt"
	"log"

	"kairolog/internal/server"
)

func main() {
	fmt.Println("KairoLog Broker Starting...")

	if err := server.Start(); err != nil {
		log.Fatalf("broker failed: %v", err)
	}
}
