package mapreduce

import (
	"fmt"
	"os/exec"
	"strings"
	. "sync"
)

type Worker struct {
	Remote     string
	ActiveJobs map[string]bool

	jobMutex Mutex
}

func NewWorker(workerRemote string) *Worker {
	return &Worker{workerRemote, make(map[string]bool), Mutex{}}
}

func (w *Worker) CheckIfJobActive(jobId string) bool {
	w.jobMutex.Lock()
	_, pres := w.ActiveJobs[jobId]
	if !pres {
		w.ActiveJobs[jobId] = true
	}
	w.jobMutex.Unlock()
	return pres
}

func (w *Worker) RemoveActiveJob(jobId string) {
	w.jobMutex.Lock()
	delete(w.ActiveJobs, jobId)
	w.jobMutex.Unlock()
}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives
// the status of the job
func (w *Worker) HandleMessage(message Message) {
	// fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case RequestWorker:
		w.HandleRequestWorker(message)
	case MapJob:
		w.HandleMapJob(message)
	case ReduceJob:
		w.HandleReduceJob(message)
	case MapRun:
		w.HandleMapRun(message)
	case ReduceRun:
		w.HandleReduceRun(message)
	case Cleanup:
		w.HandleCleanup(message)
	default:
		if message.Error != "" {
			return
		}
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func (w *Worker) HandleRequestWorker(message Message) {
	fmt.Println("This worker was requested. Sending WorkerReady message...")
	response := w.WorkerReadyMessage()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleMapJob(message Message) {
	fmt.Println("Handling Map Job")

	content := message.Message
	WriteFile(w.GetDirectory()+"/mapper.go", content)

	err := exec.Command("go", "build", "-o", "./"+w.GetDirectory()+"/mapper.exe", "./"+w.GetDirectory()+"/mapper.go").Run()
	if err != nil {
		return
	}

	response := w.MapperReady()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleMapRun(message Message) {

	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0] // Something like the job number or something
	if w.CheckIfJobActive(jobId) {
		return
	}
	value := arr[1] // The thing that needs to be mapped

	output, err := exec.Command("./"+w.GetDirectory()+"/mapper.exe", value).Output()

	if err != nil {
		// Respond with an error
		fmt.Println("The mapper has runtime errors: " + err.Error())
		w.RemoveActiveJob(jobId)
		return
	}
	// fmt.Println("Output of " + fileName + " is " + string(output))
	pairs := ParseKVPairs(string(output)) // Should be a parsing of the output

	response := w.MapResultMessage(jobId, KVPairsToString(pairs))
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleReduceJob(message Message) {
	fmt.Println("Handling Reduce Job")

	w.ActiveJobs = make(map[string]bool)
	content := message.Message
	WriteFile(w.GetDirectory()+"/reducer.go", content)
	err := exec.Command("go", "build", "-o", "./"+w.GetDirectory()+"/reducer.exe", "./"+w.GetDirectory()+"/reducer.go").Run()
	if err != nil {
		return
	}
	response := w.ReducerReady()
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleReduceRun(message Message) {

	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0] // Something like the job number or the word to count
	if w.CheckIfJobActive(jobId) {
		return
	}
	value := arr[1] // The thing that needs to be reduced

	output, err := exec.Command("./"+w.GetDirectory()+"/reducer.exe", jobId, value).Output()

	if err != nil {
		fmt.Println("The reducer has runtime errors: " + err.Error())
		w.RemoveActiveJob(jobId)
		return
	}

	// fmt.Println("Output of " + fileName + " is " + string(output))
	pair := ParseKVPair(string(output)) // Should be a parsing of the output

	response := w.ReduceResultMessage(jobId, pair.ToString())
	DialMessage(response, message.Remote)
}

func (w *Worker) HandleCleanup(message Message) {
	w.ActiveJobs = make(map[string]bool)
	exec.Command("rm", "./"+w.GetDirectory()+"/reducer.exe").Run()
	exec.Command("rm", "./"+w.GetDirectory()+"/mapper.exe").Run()
	fmt.Println("Cleanup requested. Ready for next mapreduce!")
}

func (w *Worker) GetDirectory() string {
	ip := strings.Split(w.Remote, ":")[0]
	return "worker_" + ip
}

func (w *Worker) MakeWorkerDirectory() error {
	err := exec.Command("mkdir", w.GetDirectory()).Run()
	exec.Command("rm", "./"+w.GetDirectory()+"/reducer.exe").Run()
	exec.Command("rm", "./"+w.GetDirectory()+"/mapper.exe").Run()
	return err
}
