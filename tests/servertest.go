package main

import (
	. "../../go-mapreduce"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Must provide the remote of the server as an argument.")
		os.Exit(1)
		return
	}
	controller := SetupServer(os.Args[1])
	controller.MapReduce([]KVPair{{"1", "A"}, {"2", "A"}, {"3", "B"}}, "maptest.go", "reducetest.go")
}
