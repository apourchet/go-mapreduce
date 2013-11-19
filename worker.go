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
func (w *Worker) HandleMessage(message Message) {
	// fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case MapJob:
		w.HandleMapJob(message)
	case ReduceJob:
		w.HandleReduceJob(message)
	case MapRun:
		w.HandleMapRun(message)
	case ReduceRun:
		w.HandleReduceRun(message)
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func (w *Worker) HandleMapJob(message Message) {
	fmt.Println("Handling Map Job")
}

func (w *Worker) HandleReduceJob(message Message) {
	fmt.Println("Handling Reduce Job")
}

func (w *Worker) HandleMapRun(message Message) {
	fmt.Println("Handling Map Run")
}

func (w *Worker) HandleReduceRun(message Message) {
	fmt.Println("Handling Reduce Run")
}
