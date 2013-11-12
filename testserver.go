package main

import (
	. "./socketio"
	"fmt"
)

func main() {
	inChannel := make(chan []byte)
	go Listen(inChannel, TEST_REMOTE1, false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
			Dial(TEST_REMOTE1, TEST_REMOTE2, "Server message here.")
		}
	}
}
