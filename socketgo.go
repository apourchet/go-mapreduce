package main

import (
	. "./socketio"
	"fmt"
)

type T struct {
	Msg   string
	Count int
}

func main() {
	inChannel := make(chan []byte)
	go Listen(inChannel, "127.0.0.2:9998", false)
	Dial("127.0.0.2:9998", "127.0.0.1:9998", "This is my message.")
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			fmt.Println(string(c))
		}
	}
}
