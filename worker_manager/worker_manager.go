package workermanager

import (
	. "../socketio"
	"fmt"
	"os"
	"os/exec"
)

// Does everything the message wants the worker to do
// and creates a response to the controller that gives 
// the status of the job
func HandleMessage(fromRemote string, message Message) {
	// fmt.Println("Worker Manager handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Got a test message.")
	case IO:
		HandleIOMessage(message)
	case CmdJob:
		fmt.Println("Got a Run message.")
	case MapJob:
		HandleMapMessage(fromRemote, message)
	case ReduceJob:
		fmt.Println("Got a ReduceJob message.")
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func HandleIOMessage(message Message) {
	fmt.Println("Got an IO message.")
	messageMap := GetMessageMap(message)
	switch messageMap["cmd"] {
	case "mkdir":
		fmt.Println("Making dir: " + messageMap["dirName"])
		os.Mkdir(messageMap["dirName"], os.ModePerm)
	case "create":
		fmt.Println("Creating file: " + messageMap["fileName"])
		sysfile, _ := os.Create(messageMap["fileName"])
		defer closeFile(sysfile)
		sysfile.Write([]byte{})
	case "append":
		sysfile, _ := os.OpenFile(messageMap["fileName"], os.O_RDWR|os.O_APPEND, 0660)
		defer closeFile(sysfile)
		sysfile.WriteString(messageMap["content"])
	case "remove":
		fmt.Println("Removing file: " + messageMap["fileName"])
		os.Remove(messageMap["fileName"])
	}
}

func HandleMapMessage(fromRemote string, message Message) {
	m := GetMessageMap(message)
	fmt.Println("Got a MapJob message: " + m["fileName"])
	go func() {
		o, _ := exec.Command("go", "run", m["fileName"]).Output()
		DialMessage(MapResultMessage(fromRemote, string(o)), message.Remote)
	}()
}

func closeFile(sysfile *os.File) {
	if err := sysfile.Close(); err != nil {
		panic(err)
	}
}
