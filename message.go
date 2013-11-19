package mapreduce

import (
	"strings"
)

const (
	Test        = "Test"
	WorkerReady = "WorkerReady"

	MapJob    = "MapJob"
	ReduceJob = "ReduceJob"

	MapperReady  = "MapperReady"
	ReducerReady = "ReducerReady"

	MapRun    = "MapRun"
	ReduceRun = "ReduceRun"

	MapResult    = "MapResult"
	ReduceResult = "ReduceResult"
)

type Message struct {
	Remote  string
	Type    string
	Error   string
	Message string
}

const (
	SEPARATOR = "#|#"
)

func (m *Message) ToString() string {
	return m.Remote + SEPARATOR + m.Type + SEPARATOR + m.Error + SEPARATOR + m.Message
	// return fmt.Sprintf("%s#|#%s#|#%s#|#%s", m.Remote, m.Type, m.Error, m.Message)
}
func ParseMessage(messageString string) Message {
	fstSplit := strings.Split(messageString, SEPARATOR)
	return Message{fstSplit[0], fstSplit[1], fstSplit[2], fstSplit[3]}
}

// Test message
func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

// Handled by Worker
// Sent by Controller
func (c *Controller) MapJobMessage(fileName string) Message {
	return Message{c.Remote, MapJob, "", fileName}
}

func (c *Controller) ReduceJobMessage(fileName string) Message {
	return Message{c.Remote, ReduceJob, "", "Reduce Job here"}
}

func (c *Controller) MapRunMessage(fileName string) Message {
	return Message{c.Remote, MapRun, "", fileName}
}

func (c *Controller) ReduceRunMessage(fileName string) Message {
	return Message{c.Remote, ReduceRun, "", "Reduce Job here"}
}

// Handled by Controller
// Sent by Worker
func (w *Worker) MapperReady() Message {
	return Message{w.Remote, MapperReady, "", "Mapper is Ready"}
}

func (w *Worker) ReducerReady() Message {
	return Message{w.Remote, ReducerReady, "", "Reducer is Ready"}
}

func (w *Worker) MapResultMessage(results string) Message {
	return Message{w.Remote, MapResult, "", results}
}

func (w *Worker) ReduceResultMessage(results string) Message {
	return Message{w.Remote, ReduceResult, "", "ReduceResults here"}
}

func (w *Worker) WorkerReadyMessage() Message {
	return Message{w.Remote, WorkerReady, "", "WorkerReady here"}
}
