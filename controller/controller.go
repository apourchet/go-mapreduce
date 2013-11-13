package controller

import (
	. "../socketio"
	"fmt"
	"os"
)

// Messages that the Worker Manager will handle
func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

func RunMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, Run, "", "fileName:" + fileName}
}

func CreateFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd:create#&#fileName:" + fileName}
}

func AppendFileMessage(fromRemote, fileName, additionalContent string) Message {
	return Message{fromRemote, IO, "", "cmd:append#&#fileName:" + fileName + "#&#content:" + additionalContent}
}

func RmFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd:remove#&#fileName:" + fileName}
}

func MapJobMessage(fromRemote string) Message {
	return Message{fromRemote, MapResult, "", "Map Job here"}
}

func ReduceJobMessage(fromRemote string) Message {
	return Message{fromRemote, ReduceResult, "", "Reduce Job here"}
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
	os.Mkdir("worker_"+message.Remote, os.ModePerm)
	DialMessage(CreateFileMessage(fromRemote, "data/testFile.txt"), message.Remote)
	DialMessage(AppendFileMessage(fromRemote, "data/testFile.txt", "This is the first append!\n"), message.Remote)
	DialMessage(AppendFileMessage(fromRemote, "data/testFile.txt", "This is the second append!\n"), message.Remote)
	DialMessage(RmFileMessage(fromRemote, "data/testFile.txt"), message.Remote)
}
