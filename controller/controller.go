package controller

import (
	. "../socketio"
)

// Messages that the Worker Manager will handle
func RunMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, Run, "", "fileName:" + fileName}
}

func CreateFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "fileName:" + fileName}
}

func AppendFileMessage(fromRemote, fileName, additionalContent string) Message {
	return Message{fromRemote, IO, "", "fileName:" + fileName + "#&#" + additionalContent}
}

func RmFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "fileName:" + fileName}
}

func MapJobMessage(fromRemote string) Message {
	return Message{fromRemote, MapResult, "", ""}
}

func ReduceJobMessage(fromRemote string) Message {
	return Message{fromRemote, ReduceResult, "", ""}
}

// Handles the message from a Worker after sending a job to it
func HandleMessage(fromRemote string, message Message) {
	fmt.Println("Controller handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println(message.Message)
	case MapResult:
		fmt.Println(message.Message)
	case ReduceResult:
		fmt.Println(message.Message)
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func HandleNewWorker(message Message) {

}
