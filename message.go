package mapreduce

import (
	"fmt"
	"strings"
)

const (
	Test          = "Test"
	WorkerReady   = "WorkerReady"
	RequestWorker = "RequestWorker"

	MapJob    = "MapJob"
	ReduceJob = "ReduceJob"

	MapperReady  = "MapperReady"
	ReducerReady = "ReducerReady"

	MapRun    = "MapRun"
	ReduceRun = "ReduceRun"

	MapResult    = "MapResult"
	ReduceResult = "ReduceResult"

	Cleanup = "Cleanup"
	Fatal   = "Fatal"
)

type Message struct {
	Remote  string
	Type    string
	Error   string
	Message string
}

const (
	SEPARATOR = "#|#"
	ARGSEP    = "#&#"
	MSGSEP    = "#$#"
)

func (m *Message) ToString() string {
	return m.Remote + SEPARATOR + m.Type + SEPARATOR + m.Error + SEPARATOR + m.Message + MSGSEP
	// return fmt.Sprintf("%s#|#%s#|#%s#|#%s", m.Remote, m.Type, m.Error, m.Message)
}
func ParseMessage(messageString string) Message {
	fstSplit := strings.Split(messageString, SEPARATOR)
	if len(fstSplit) < 4 {
		// fmt.Println("Error parsing: " + messageString)
		fmt.Println("Error parsing message...")
		return Message{"", "", "Error parsing", ""}
	}
	return Message{fstSplit[0], fstSplit[1], fstSplit[2], fstSplit[3]}
}
func ParseMessages(messageStrings string) []Message {
	msgs := []Message{}
	strs := strings.Split(messageStrings, MSGSEP)
	for _, messageString := range strs {
		if messageString == "" {
			continue
		}
		newMsg := ParseMessage(messageString)
		if newMsg.Error != "" {
			continue
		}
		msgs = append(msgs, newMsg)
	}
	return msgs
}

// Test message
func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

// Handled by Worker
// Sent by Controller
func (c *Controller) MapJobMessage(fileContent string) Message {
	return Message{c.Remote, MapJob, "", fileContent}
}

func (c *Controller) ReduceJobMessage(fileContent string) Message {
	return Message{c.Remote, ReduceJob, "", fileContent}
}

func (c *Controller) MapRunMessage(key, value string) Message {
	return Message{c.Remote, MapRun, "", key + ARGSEP + value}
}

func (c *Controller) ReduceRunMessage(key, value string) Message {
	return Message{c.Remote, ReduceRun, "", key + ARGSEP + value}
}

func (c *Controller) RequestWorkerMessage() Message {
	return Message{c.Remote, RequestWorker, "", ""}
}

func (c *Controller) CleanupMessage() Message {
	return Message{c.Remote, Cleanup, "", ""}
}

// Handled by Controller
// Sent by Worker
func (w *Worker) MapperReady() Message {
	return Message{w.Remote, MapperReady, "", "Mapper is Ready"}
}

func (w *Worker) ReducerReady() Message {
	return Message{w.Remote, ReducerReady, "", "Reducer is Ready"}
}

func (w *Worker) MapResultMessage(jobId, results string) Message {
	return Message{w.Remote, MapResult, "", jobId + ARGSEP + results}
}

func (w *Worker) ReduceResultMessage(jobId, results string) Message {
	return Message{w.Remote, ReduceResult, "", jobId + ARGSEP + results}
}

func (w *Worker) WorkerReadyMessage() Message {
	return Message{w.Remote, WorkerReady, "", "WorkerReady here"}
}

func FatalMessage() Message {
	return Message{"", Fatal, "", ""}
}
