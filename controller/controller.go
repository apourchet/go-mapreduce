package controller

import (
	. "../socketio"
	"../worker_manager"
)

func MapReturnMessage(fromRemote string) Message {
	return Message{}
}

func CombineReturnMessage(fromRemote string) Message {
	return Message{}
}

func ReduceReturnMessage(fromRemote string) Message {
	return Message{}
}

func HandleMessage(message Message) {
	fmt.Println("Controller Handling message:\n" + message.ToString())
}
