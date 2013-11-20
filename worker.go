package mapreduce

import (
	"fmt"
	"os/exec"
	"strings"
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
	// TODO Handle the map job here, i.e save the script onto the system in the designated folder

	// Respond to the server saying that the worker is ready to map
	fmt.Println("MapperReady response sent!")
	response := w.MapperReady()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleMapRun(message Message) {
	fmt.Println("Handling Map Run")
	// Handle the run here
	arr := strings.Split(message.Message, ARGSEP)
	fileName := w.GetDirectory() + "/" + arr[0]
	key := arr[1]
	value := arr[2]
	pairs := []KVPair{{"2", "DEFAULT"}}
	// TODO Run the script using this key-value pair && convert the resulting kv pairs to a string
	exec.Command("go", "run", fileName, key, value)

	// TODO Respond to the server with the result of the map
	fmt.Println("MapperResults sent!")
	response := w.MapResultMessage(KVPairsToString(pairs))
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleReduceJob(message Message) {
	fmt.Println("Handling Reduce Job")
}

func (w *Worker) HandleReduceRun(message Message) {
	fmt.Println("Handling Reduce Run")
}

func (w *Worker) GetDirectory() string {
	ip := strings.Split(w.Remote, ":")[0]
	return "worker_" + ip
}

func (w *Worker) MakeWorkerDirectory() error {
	return exec.Command("mkdir", w.GetDirectory()).Run()
}
