package workermanager

import (
	. "../socketio"
)

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

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(message Message) {

}
