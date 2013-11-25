package main

import (
	"fmt"
	"os"
	"regexp"
)

func main() {
	value := os.Args[1]
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	ret := "{"
	for i, word := range reg.Split(value, -1) {
		if i > 0 {
			ret += ";"
		}
		ret += "{" + word + ",1}"
	}
	ret += "}"
	// fmt.Println("I am mapping! " + os.Args[1])
	fmt.Print(ret)
}
