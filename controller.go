package mapreduce

import (
	"bufio"
	"fmt"
	"os"
)

type Controller struct {
	Remote string
}

// Handles the message from a Worker after sending a job to it
func (c *Controller) HandleMessage(message Message) {
	// fmt.Println("Controller handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Handling Test message")
		DialMessage(TestMessage(fromRemote, "Response to test message"), message.Remote)
	case WorkerReady:
		HandleNewWorker(fromRemote, message)
	case MapResult:
		HandleMapResults(fromRemote, message)
	case ReduceResult:
		HandleReduceResults(fromRemote, message)
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func (c *Controller) HandleNewWorker(fromRemote string, message Message) {
	fmt.Println("Worker is ready on remote: " + message.Remote)
	workerDir := getWorkerDir(message)
}

func (c *Controller) HandleMapResults(fromRemote string, message Message) {
	fmt.Println("Map Results have arrived!")
}

func (c *Controller) HandleReduceResults(fromRemote string, message Message) {
	fmt.Println("Reduce Results have arrived!")
	fmt.Println(message.Message)
}

func getWorkerDir(message Message) string {
	return "worker_" + message.Remote + "/"
}
