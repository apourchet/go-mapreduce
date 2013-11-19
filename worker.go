package mapreduce

import (
	"fmt"
)

type Worker struct {
	Remote string
}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives
// the status of the job
func (w *Worker) HandleMessage(fromRemote string, message Message) {
	// fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case IO:
		HandleIOMessage(message)
	case CmdJob:
		fmt.Println("Got a Run message.")
	case MapJob:
		HandleMapMessage(fromRemote, message)
	case ReduceJob:
		fmt.Println("Got a ReduceJob message.")
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}
