package socketio

import (
	"strings"
)

// Messages that the Worker Manager will handle
func TestMessage(fromRemote, msg string) Message {
	return Message{fromRemote, Test, "", msg}
}

func CmdJobMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, CmdJob, "", "fileName#@#" + fileName}
}

func MakeDirMessage(fromRemote, dirName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#mkdir#&#dirName#@#" + dirName}
}

func CreateFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#create#&#fileName#@#" + fileName}
}

func AppendFileMessage(fromRemote, fileName, additionalContent string) Message {
	return Message{fromRemote, IO, "", "cmd#@#append#&#fileName#@#" + fileName + "#&#content#@#" + additionalContent}
}

func RmFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IO, "", "cmd#@#remove#&#fileName#@#" + fileName}
}

func MapJobMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, MapJob, "", "fileName#@#" + fileName}
}

func ReduceJobMessage(fromRemote string) Message {
	return Message{fromRemote, ReduceJob, "", "Reduce Job here"}
}

// Messages that the Controller will be able to handle
func MapResultMessage(fromRemote, results string) Message {
	return Message{fromRemote, MapResult, "", results}
}

func ReduceResultMessage(fromRemote, results string) Message {
	// return Message{fromRemote, JobResultType, "", "ReduceResults here"}
	return Message{fromRemote, ReduceResult, "", "ReduceResults here"}
}

func WorkerReadyMessage(fromRemote string) Message {
	return Message{fromRemote, WorkerReady, "", "WorkerReady here"}
}

func GetMessageMap(message Message) map[string]string {
	split := strings.Split(message.Message, "#&#")
	m := make(map[string]string)
	for _, s := range split {
		split2 := strings.Split(s, "#@#")
		m[split2[0]] = split2[1]
	}
	return m
}
