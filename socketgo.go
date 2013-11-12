package main

import (
	. "./socketio"
	"fmt"
)

func main() {
	inChannel := make(chan []byte)
	go Listen(inChannel, TEST_REMOTE2, false)
	Dial(TEST_REMOTE2, TEST_REMOTE1, "Client message here.")
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
		}
	}
}
