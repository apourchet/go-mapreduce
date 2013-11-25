package main

import (
	"fmt"
	"os"
	// "time"
)

func main() {
	key := os.Args[1]
	// value := os.Args[2]
	fmt.Print("{" + key + "," + "1" + "}")
}
