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

	inChannel := make(chan Message)
	controller := NewController(serverRemote)

	requestWorker := controller.RequestWorkerMessage()
	for _, workerRemote := range workerRemotes {
		fmt.Println("Worker Remote: " + workerRemote)
		outChannel := make(chan Message)
		go DialAndListen(workerRemote, inChannel, outChannel)
		go func() {
			outChannel <- requestWorker
		}()
	}
	fmt.Println("ServerSetup: finished setting up...")
	go func() {
		for {
			fmt.Println("ServerSetup: reading inChannel...")
			for c := <-inChannel; c.Type != Fatal; c = <-inChannel {
				fmt.Println("Controller will be handling this message!")
				go controller.HandleMessage(c)
			}
			fmt.Println("ServerSetup: Finished reading inChannel...")
		}
	}()
	return controller
}

func SetupWorkerStandby(workerRemote string) *Worker {
	fmt.Println("\n******Worker initializing******")
	fmt.Println("Worker Remote: " + workerRemote)

	inChannel := make(chan Message)
	outChannel := make(chan Message)

	worker := NewWorker(workerRemote, outChannel)
	err := worker.MakeWorkerDirectory()
	if err != nil {
		fmt.Println("Could not make worker directory: \n" + err.Error())
	}

	go ListenStream(inChannel, outChannel, workerRemote)
	for {
		for c := <-inChannel; c.Type != Fatal; c = <-inChannel {
			go worker.HandleMessage(c)
		}
	}
	return worker
}
