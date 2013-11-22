package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	value := os.Args[1]
	ret := "{"
	for i, word := range strings.Split(value, " ") {
		if i > 0 {
			ret += ";"
		}
		ret += "{" + word + ",1}"
	}
	ret += "}"
	// fmt.Println("I am mapping! " + os.Args[1])
	fmt.Print(ret)
}
