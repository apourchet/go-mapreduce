package mapreduce

import (
	"fmt"
	// "os"
	// "os/exec"
	// "strings"
)

func SetupServerWithWorkers(serverRemote string, workerRemotes []string) *Controller {
	fmt.Println("\n******Server initializing******")
	fmt.Println("Server Remote: " + serverRemote)

	inChannel := make(chan Message, 100)
	controller := NewController(serverRemote)

	requestWorker := controller.RequestWorkerMessage()
	for _, workerRemote := range workerRemotes {
		fmt.Println("Worker Remote: " + workerRemote)
		outChannel := make(chan Message)
		controller.AddPendingWorker(workerRemote, outChannel)
		go DialAndListen(workerRemote, inChannel, outChannel)
		go func() {
			outChannel <- requestWorker
		}()
	}
	go func() {
		for {
			for c := <-inChannel; c.Type != Fatal; c = <-inChannel {
				go controller.HandleMessage(c)
			}
		}
	}()
	return controller
}

func SetupWorkerStandby(workerRemote string) *Worker {
	fmt.Println("\n******Worker initializing******")
	fmt.Println("Worker Remote: " + workerRemote)

	inChannel := make(chan Message, 100)
	outChannel := make(chan Message, 100)

	worker := NewWorker(workerRemote, outChannel)
	go ListenStream(inChannel, outChannel, workerRemote)
	err := worker.MakeWorkerDirectory()
	if err != nil {
		fmt.Println("Could not make worker directory: \n" + err.Error())
	}

	for {
		for c := <-inChannel; c.Type != Fatal; c = <-inChannel {
			go worker.HandleMessage(c)
		}
	}
	return worker
}
