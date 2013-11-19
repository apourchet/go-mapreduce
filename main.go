package mapreduce

import (
	"fmt"
)

func SetupServer(serverRemote string) {
	fmt.Println("\n******Server initializing******")
	inChannel := make(chan []byte)
	go Listen(inChannel, serverRemote, false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			m := ParseMessage(string(c))
			go HandleMessage(serverRemote, *m)
		}
	}
}

func SetupWorker(workerRemote, serverRemote string) {
	fmt.Println("\n******Worker initializing******")
	inChannel := make(chan []byte)
	go Listen(inChannel, workerRemote, false)
	DialMessage(WorkerReadyMessage(thisRemote), otherRemote)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			m := ParseMessage(string(c))
			HandleMessage(*m)
		}
	}
}
