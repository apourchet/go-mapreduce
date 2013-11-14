package controller

import (
	. "../socketio"
	"bufio"
	"fmt"
	"os"
)

// Messages that the Worker Manager will handle
func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

func CmdJobMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, CmdJob, "", "fileName#@#" + fileName}
}

func MakeDirMessage(fromRemote, dirName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#mkdir#&#dirName#@#" + dirName}
}

func CreateFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#create#&#fileName#@#" + fileName}
}

func AppendFileMessage(fromRemote, fileName, additionalContent string) Message {
	return Message{fromRemote, IO, "", "cmd#@#append#&#fileName#@#" + fileName + "#&#content#@#" + additionalContent}
}

func RmFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#remove#&#fileName#@#" + fileName}
}

func MapJobMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, MapJob, "", "fileName#@#" + fileName}
}

func ReduceJobMessage(fromRemote string) Message {
	return Message{fromRemote, ReduceJob, "", "Reduce Job here"}
}

// Handles the message from a Worker after sending a job to it
func HandleMessage(fromRemote string, message Message) {
	// fmt.Println("Controller handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Handling Test message")
		DialMessage(TestMessage(fromRemote, "Response to test message"), message.Remote)
	case WorkerReady:
		fmt.Println("Worker is ready on remote: " + message.Remote)
		HandleNewWorker(fromRemote, message)
	case MapResult:
		fmt.Println("Map Results have arrived!")
	case ReduceResult:
		fmt.Println("Reduce Results have arrived!")
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func HandleNewWorker(fromRemote string, message Message) {
	workerDir := getWorkerDir(message)
	fmt.Println("Making dir: " + workerDir)
	DialMessage(MakeDirMessage(fromRemote, workerDir), message.Remote)
	SendFileWorker(fromRemote, workerDir+"testscript.go", message)
	DialMessage(MapJobMessage(fromRemote, workerDir+"testscript.go"), message.Remote)
}

func HandleMapResults(fromRemote string, message Message) {

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
