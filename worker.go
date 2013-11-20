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
	content := message.Message
	WriteFile(w.GetDirectory()+"/mapper.go", content)
	// Respond to the server saying that the worker is ready to map
	fmt.Println("MapperReady response sent!")
	response := w.MapperReady()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleMapRun(message Message) {
	fmt.Println("Handling Map Run")
	// Handle the run here
	arr := strings.Split(message.Message, ARGSEP)
	fileName := w.GetDirectory() + "/mapper.go"
	jobId := arr[0] // Something like the job number or something
	value := arr[1] // The thing that needs to be reduced

	output, err := exec.Command("go", "run", fileName, value).Output()
	if err != nil {
		// Respond with an error
		fmt.Println("The mapper has runtime errors: " + err.Error())
		return
	}
	fmt.Println("Output of " + fileName + " is " + string(output))
	pairs := ParseKVPairs(string(output)) // Should be a parsing of the output

	fmt.Println("MapperResults sent!")
	response := w.MapResultMessage(jobId, KVPairsToString(pairs))
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleReduceJob(message Message) {
	fmt.Println("Handling Reduce Job")
	// TODO Handle the map job here, i.e save the script onto the system in the designated folder
	content := message.Message
	WriteFile(w.GetDirectory()+"/reducer.go", content)
	// Respond to the server saying that the worker is ready to map
	fmt.Println("ReducerReady response sent!")
	response := w.ReducerReady()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleReduceRun(message Message) {
	fmt.Println("Handling Reduce Run")
	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0] // Something like the job number or something

	fmt.Println("ReduceResults sent!")
	response := w.ReduceResultMessage(jobId, "{1,Reduced}")
	DialMessage(response, message.Remote)
}

func (w *Worker) GetDirectory() string {
	ip := strings.Split(w.Remote, ":")[0]
	return "worker_" + ip
}

func (w *Worker) MakeWorkerDirectory() error {
	return exec.Command("mkdir", w.GetDirectory()).Run()
}
