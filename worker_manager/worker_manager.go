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
	return Message{fromRemote, ReduceResult, "", "ReduceResults here"}
}

func WorkerReadyMessage(fromRemote string) Message {
	return Message{fromRemote, WorkerReady, "", "WorkerReady here"}
}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(fromRemote string, message Message) {
	fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case IO:
		fmt.Println("Got an IO message.")
	case Run:
		fmt.Println("Got a Run message.")
	case MapJob:
		fmt.Println("Got a MapJob message.")
	case ReduceJob:
		fmt.Println("Got a ReduceJob message.")
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
	// os.OpenFile("foo.txt", os.O_RDWR|os.O_APPEND, 0660)
}
