package controller

import (
	. "../socketio"
	"../worker_manager"
)

func MapReturnMessage(fromRemote string) Message {
	return Message{fromRemote}
}

func CombineReturnMessage(fromRemote string) Message {
	return Message{fromRemote}
}

func ReduceReturnMessage(fromRemote string) Message {
	return Message{fromRemote}
}

// Handles the message from a Worker after sending a job to it
func HandleMessage(message Message) {
	fmt.Println("Controller Handling message:\n" + message.ToString())
}
