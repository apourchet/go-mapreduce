package main

import (
	"fmt"
	"os"
)

func main() {
	value := os.Args[1]
	// fmt.Println("I am mapping! " + os.Args[1])
	fmt.Print("{{1," + value + "}}")
}
