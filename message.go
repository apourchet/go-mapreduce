package mapreduce

import (
	"fmt"
)

const (
	Test         = "Test"
	WorkerReady  = "WorkerReady"
	CmdJob       = "CmdJob"
	MapJob       = "MapJob"
	ReduceJob    = "ReduceJob"
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
func ParseMessage(messageString string) *Message {
	fstSplit := strings.Split(messageString, SEPARATOR)
	msg := Message{fstSplit[0], fstSplit[1], fstSplit[2], fstSplit[3]}
	return &msg
}

func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

// Messages handled by the Workers
func (c *Controller) MapJobMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, MapJob, "", fileName}
}

func (c *Controller) ReduceJobMessage(fromRemote string) Message {
	return Message{fromRemote, ReduceJob, "", "Reduce Job here"}
}

// Messages that the Controller will be able to handle
func (w *Worker) MapResultMessage(fromRemote, results string) Message {
	return Message{fromRemote, MapResult, "", results}
}

func (w *Worker) ReduceResultMessage(fromRemote, results string) Message {
	return Message{fromRemote, ReduceResult, "", "ReduceResults here"}
}

func (w *Worker) WorkerReadyMessage(fromRemote string) Message {
	return Message{fromRemote, WorkerReady, "", "WorkerReady here"}
}
