package main

import (
	. "../../go-mapreduce"
	"bufio"
	"fmt"
	"os"
	"regexp"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Must provide the remote of the server as an argument.")
		os.Exit(1)
		return
	}
	// controller := SetupServer(os.Args[1])
	controller := SetupServerWithWorkers(os.Args[1], []string{"127.0.0.2:8080", "127.0.0.3:8080"})

	fi, err := os.Open("reuters.txt")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(fi)
	reg, _ := regexp.Compile("@")
	pairs := []KVPair{}
	for buffer, _, err := reader.ReadLine(); err == nil; buffer, _, err = reader.ReadLine() {
		line := string(buffer)
		arr := reg.Split(line, -1)
		if len(arr) == 3 {
			pairs = append(pairs, KVPair{arr[0], arr[2]})
		}
	}

	// controller.MapReduce(pairs, "maptest.go", "reducetest.go")
	controller.MapReduce([]KVPair{{"1", "A B C"}, {"2", "A"}, {"3", "B"}}, "maptest.go", "reducetest.go")
}
