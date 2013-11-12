package socketio

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

const (
	TestType = "TestType"
)

type Message struct {
	Remote  string
	Type    string
	Id      string
	Error   string
	Message string
}

func (m *Message) ToString() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", m.Remote, m.Type, m.Id, m.Error, m.Message)
}

func GetRemote(host, port string) string {
	return host + ":" + port
}

func ParseMessage(messageString string) *Message {
	fstSplit := strings.Split(messageString, "|")
	msg := Message{fstSplit[0], fstSplit[1], fstSplit[2], fstSplit[3], fstSplit[4]}
	return &msg
}

func Listen(inChannel chan []byte, remote string, closeChannel bool) {
	lis, err := net.Listen("tcp", remote)
	if err != nil {
		inChannel <- []byte{}
		return
	}
	defer lis.Close()
	fmt.Println("Listening on remote: " + remote)
	data := make([]byte, 5)
	for {
		output := bytes.NewBuffer([]byte{})
		con, err := lis.Accept()
		if err != nil {
			continue
		}
		for n, err := con.Read(data); err == nil; n, err = con.Read(data) {
			output.Write(data[0:n])
		}
		inChannel <- output.Bytes()
		if closeChannel {
			close(inChannel)
		}
		con.Close()
	}
}

func Dial(fromRemote, toRemote, msg string) {
	message := Message{}
	message.Remote = fromRemote
	message.Type = TestType
	message.Id = "-1"
	message.Error = ""
	message.Message = msg
	con, err := net.Dial("tcp", toRemote)
	defer con.Close()
	if err != nil {
		fmt.Printf("Host not found: %s\n", err)
		return
	}
	fmt.Println("Dialing remote: " + toRemote)
	in, err := con.Write([]byte(message.ToString()))
	if err != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", err, in)
	}
}
