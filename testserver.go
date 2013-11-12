package main

import (
	. "./socketio"
	"fmt"
)

func main() {
	inChannel := make(chan []byte)
	go Listen(inChannel, "127.0.0.1", "9998")
	for c := <-inChannel; len(c) != 0; c = <-inChannel {
		fmt.Println(string(c))
	}
}
