package controller

import (
	. "../socketio"
	"bufio"
	"fmt"
	"os"
)

// Handles the message from a Worker after sending a job to it
func HandleMessage(fromRemote string, message Message) {
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

func HandleNewWorker(fromRemote string, message Message) {
	fmt.Println("Worker is ready on remote: " + message.Remote)
	workerDir := getWorkerDir(message)
	fmt.Println("Making dir: " + workerDir)
	DialMessage(MakeDirMessage(fromRemote, workerDir), message.Remote)
	SendFileWorker(fromRemote, workerDir+"testscript.go", message)
	DialMessage(MapJobMessage(fromRemote, workerDir+"testscript.go"), message.Remote)
}

func HandleMapResults(fromRemote string, message Message) {
	fmt.Println("Map Results have arrived!")
	// m := GetMessageMap(message)
	fmt.Println(message.Message)
}

func HandleReduceResults(fromRemote string, message Message) {
	fmt.Println("Reduce Results have arrived!")
}

func getWorkerDir(message Message) string {
	return "worker_" + message.Remote + "/"
}

func SendFileWorker(fromRemote, fileName string, message Message) {
	fi, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(fi)
	DialMessage(CreateFileMessage(fromRemote, fileName), message.Remote)
	for buffer, _, err := reader.ReadLine(); err == nil; buffer, _, err = reader.ReadLine() {
		DialMessage(AppendFileMessage(fromRemote, fileName, string(buffer)+"\n"), message.Remote)
	}
}

func RemoveFileWorker(fromRemote, fileName string, message Message) {
	DialMessage(RmFileMessage(fromRemote, getWorkerDir(message)+fileName), message.Remote)
}
