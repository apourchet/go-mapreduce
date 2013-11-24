package mapreduce

import (
	"fmt"
	// "os"
	// "os/exec"
	// "strings"
)

func SetupServer(serverRemote string) *Controller {
	fmt.Println("\n******Server initializing******")
	fmt.Println("Server Remote: " + serverRemote)
	inChannel := make(chan []byte)
	controller := NewController(serverRemote)

	go Listen(inChannel, serverRemote, false)
	go func() {
		for {
			for c := <-inChannel; len(c) != 0; c = <-inChannel {
				msgs := ParseMessages(string(c))
				for _, m := range msgs {
					go controller.HandleMessage(m)
				}
			}
		}
	}()
	return controller
}

func SetupServerWithWorkers(serverRemote string, workerRemotes []string) *Controller {
	fmt.Println("\n******Server initializing******")
	fmt.Println("Server Remote: " + serverRemote)
	inChannel := make(chan []byte)
	controller := NewController(serverRemote)

	go Listen(inChannel, serverRemote, false)
	requestWorker := controller.RequestWorkerMessage()
	for _, workerRemote := range workerRemotes {
		fmt.Println("Worker Remote: " + workerRemote)
		DialMessage(requestWorker, workerRemote)
	}

	go func() {
		for {
			for c := <-inChannel; len(c) != 0; c = <-inChannel {
				msgs := ParseMessages(string(c))
				for _, m := range msgs {
					go controller.HandleMessage(m)
				}
			}
		}
	}()
	return controller
}

func SetupWorker(workerRemote, serverRemote string) *Worker {
	fmt.Println("\n******Worker initializing******")
	fmt.Println("Server Remote: " + serverRemote)
	fmt.Println("Worker Remote: " + workerRemote)
	inChannel := make(chan []byte)
	worker := NewWorker(workerRemote)
	err := worker.MakeWorkerDirectory()
	if err != nil {
		fmt.Println("Could not make worker directory: \n" + err.Error())
		// os.Exit(1)
	}
	readyMessage := worker.WorkerReadyMessage()

	go Listen(inChannel, workerRemote, false)
	DialMessage(readyMessage, serverRemote)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			msgs := ParseMessages(string(c))
			for _, m := range msgs {
				go worker.HandleMessage(m)
			}
		}
	}
	return worker
}

func SetupWorkerStandby(workerRemote string) *Worker {
	fmt.Println("\n******Worker initializing******")
	fmt.Println("Worker Remote: " + workerRemote)
	inChannel := make(chan []byte)
	worker := NewWorker(workerRemote)
	err := worker.MakeWorkerDirectory()
	if err != nil {
		fmt.Println("Could not make worker directory: \n" + err.Error())
		// os.Exit(1)
	}

	go Listen(inChannel, workerRemote, false)
	for {
		for c := <-inChannel; len(c) != 0; c = <-inChannel {
			msgs := ParseMessages(string(c))
			for _, m := range msgs {
				go worker.HandleMessage(m)
			}
		}
	}
	return worker
}
