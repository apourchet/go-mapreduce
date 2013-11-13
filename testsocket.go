package main

import (
	. "./socketio"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: <worker remote> <server remote>")
		return
	}
	inChannel := make(chan []byte)
	go Listen(inChannel, os.Args[1], false)
	Dial(os.Args[1], os.Args[2], "Client message here.")
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
		}
	}
}
