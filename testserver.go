package main

import (
	. "./socketio"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: <server remote>")
		return
	}
	inChannel := make(chan []byte)
	go Listen(inChannel, os.Args[1], false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			m := ParseMessage(string(c))
			fmt.Println("Message from worker: " + m.Message)
			Dial(os.Args[1], m.Remote, "Server message here.")
		}
	}
}
