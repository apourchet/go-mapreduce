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
	go Listen(inChannel, TEST_REMOTE1, false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
			Dial(TEST_REMOTE1, TEST_REMOTE2, "Server message here.")
		}
	}
}
