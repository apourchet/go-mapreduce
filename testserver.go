package main

import (
	. "./controller"
	. "./socketio"
	"fmt"
	"os"
)

func main() {
	fmt.Println("\n******Server initializing******")
	if len(os.Args) < 2 {
		fmt.Println("Usage: <server remote>")
		return
	}
	thisRemote := os.Args[1]
	inChannel := make(chan []byte)
	go Listen(inChannel, thisRemote, false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			m := ParseMessage(string(c))
			go HandleMessage(thisRemote, *m)
		}
	}
}
