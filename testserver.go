package main

import (
	. "./socketio"
	"fmt"
)

// IP from CSUG: 128.84.127.14
func main() {
	inChannel := make(chan []byte)
	go Listen(inChannel, "127.0.0.1:9998", false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
			Dial("127.0.0.1:9998", "127.0.0.2:9998", "Server message here.")
		}
	}
}
