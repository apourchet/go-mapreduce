package mapreduce

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func SetupServer(serverRemote string) *Controller {
	fmt.Println("\n******Server initializing******")
	fmt.Println("Server Remote: " + serverRemote)
	inChannel := make(chan []byte)
	controller := Controller{serverRemote, []string{}, []string{}, []string{}}

	go Listen(inChannel, serverRemote, false)
	go func() {
		for {
			for c := <-inChannel; len(c) != 0; c = <-inChannel {
				m := ParseMessage(string(c))
				go controller.HandleMessage(m)
			}
		}
	}()
	return &controller
}

func SetupWorker(workerRemote, serverRemote string) *Worker {
	fmt.Println("\n******Worker initializing******")
	fmt.Println("Server Remote: " + serverRemote)
	fmt.Println("Worker Remote: " + workerRemote)
	inChannel := make(chan []byte)
	worker := Worker{workerRemote}
	err := MakeWorkerDirectory(workerRemote)
	if err != nil {
		fmt.Println("Could not make worker directory: \n" + err.Error())
		os.Exit(1)
	}
	readyMessage := worker.WorkerReadyMessage()

	go Listen(inChannel, workerRemote, false)
	DialMessage(readyMessage, serverRemote)
	go func() {
		for {
			for c := <-inChannel; len(c) != 0; c = <-inChannel {
				m := ParseMessage(string(c))
				go worker.HandleMessage(m)
			}
		}
	}()
	return &worker
}

func MakeWorkerDirectory(workerRemote string) error {
	ip := strings.Split(workerRemote, ":")[0]
	return exec.Command("mkdir", "worker_"+ip).Run()
}
