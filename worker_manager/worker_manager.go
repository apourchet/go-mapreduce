package workermanager

import (
	. "../socketio"
	"fmt"
	"os"
	"strings"
)

// Messages that the Controller will be able to handle
func MapResultMessage(fromRemote string) Message {
	return Message{fromRemote, MapResult, "", "MapResults here"}
}

func ReduceResultMessage(fromRemote string) Message {
	// return Message{fromRemote, JobResultType, "", "ReduceResults here"}
	return Message{fromRemote, ReduceResult, "", "ReduceResults here"}
}

func WorkerReadyMessage(fromRemote string) Message {
	return Message{fromRemote, WorkerReady, "", "WorkerReady here"}
}

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(fromRemote string, message Message) {
	// fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case IO:
		fmt.Println("Got an IO message.")
		HandleIOMessage(message)
	case Run:
		fmt.Println("Got a Run message.")
	case MapJob:
		fmt.Println("Got a MapJob message.")
	case ReduceJob:
		fmt.Println("Got a ReduceJob message.")
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
	// os.OpenFile("foo.txt", os.O_RDWR|os.O_APPEND, 0660)
}

func GetMessageMap(message Message) map[string]string {
	split := strings.Split(message.Message, "#&#")
	m := make(map[string]string)
	for _, s := range split {
		split2 := strings.Split(s, ":")
		m[split2[0]] = split2[1]
	}
	return m
}

func HandleIOMessage(message Message) {
	messageMap := GetMessageMap(message)
	switch messageMap["cmd"] {
	case "create":
		fmt.Println("Creating file: " + messageMap["fileName"])
		sysfile, _ := os.Create(messageMap["fileName"])
		defer func() {
			if err := sysfile.Close(); err != nil {
				panic(err)
			}
		}()
		sysfile.Write([]byte{})
	case "append":
		fmt.Println("Appending content: " + messageMap["content"])
		sysfile, _ := os.OpenFile(messageMap["fileName"], os.O_RDWR|os.O_APPEND, 0660)
		defer func() {
			if err := sysfile.Close(); err != nil {
				panic(err)
			}
		}()
		sysfile.WriteString(messageMap["content"])
	case "remove":
		fmt.Println("Removing file: " + messageMap["fileName"])
		os.Remove(messageMap["fileName"])
	}
}
