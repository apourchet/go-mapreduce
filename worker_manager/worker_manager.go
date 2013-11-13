package workermanager

import (
	. "../socketio"
	"fmt"
)

// Messages that the Controller will be able to handle
func MapResultMessage(fromRemote string) Message {
	return Message{fromRemote, MapResult, "", "MapResults here"}
}

func ReduceResultMessage(fromRemote string) Message {
	// return Message{fromRemote, JobResultType, "", "ReduceResults here"}
	return Message{}
}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(fromRemote string, message Message) {
	fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println(message.Message)
	case IO:
		fmt.Println(message.Message)
	case Run:
		fmt.Println(message.Message)
	case MapJob:
		fmt.Println(message.Message)
	case ReduceJob:
		fmt.Println(message.Message)
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
	// os.OpenFile("foo.txt", os.O_RDWR|os.O_APPEND, 0660)
}
