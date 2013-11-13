package main

import (
	. "./socketio"
	. "./worker_manager"
	"fmt"
	"os"
)

func main() {
	fmt.Println("\n******Worker initializing******")
	if len(os.Args) < 3 {
		fmt.Println("Usage: <worker remote> <server remote>")
		return
	}
	thisRemote := os.Args[1]
	otherRemote := os.Args[2]
	inChannel := make(chan []byte)
	go Listen(inChannel, thisRemote, false)
	// Dial(thisRemote, otherRemote, "Worker message here.")
	DialMessage(WorkerReadyMessage(thisRemote), otherRemote)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			m := ParseMessage(string(c))
			HandleMessage(thisRemote, *m)
		}
	}
}
