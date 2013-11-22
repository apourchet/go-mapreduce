package main

import (
	. "../../go-mapreduce"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Must provide the remote of the worker and server as arguments.")
		os.Exit(1)
		return
	}
	SetupWorkerStandby(os.Args[1])
	// SetupWorker(os.Args[1], os.Args[2])
}
