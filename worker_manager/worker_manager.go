package workermanager

import (
	"../controller"
	. "../socketio"
)

// Messages that will be handled by the Worker Manager
func RunMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, RunType, "", "fileName:" + fileName}
}

func CreateFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IOType, "", "fileName:" + fileName}
}

func AppendFileMessage(fromRemote, fileName, additionalContent string) Message {
	return Message{fromRemote, IOType, "", "fileName:" + fileName + "#&#" + additionalContent}
}

func RmFileMessage(fromRemote, fileName string) Message {
	return Message{fromRemote, IOType, "", "fileName:" + fileName}
}

func MapJobMessage(fromRemote string) Message {

}

func CombineJobMessage(fromRemote string) Message {

}

func ReduceJobMessage(fromRemote string) Message {

}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(message Message) {
	os.OpenFile("foo.txt", os.O_RDWR|os.O_APPEND, 0660)
}
