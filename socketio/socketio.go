package socketio

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

const (
	TestType      = "TestType"
	IOType        = "IOType"
	RunType       = "RunType"
	JobType       = "JobType"
	JobResultType = "JobResultType"
	CSUG_IP       = "128.84.127.14"
	CSUG_REMOTE   = "128.84.127.14:9998"
	TEST_REMOTE1  = "127.0.0.1:9998"
	TEST_REMOTE2  = "127.0.0.2:9998"
)

type Message struct {
	Remote  string
	Type    string
	Error   string
	Message string
}

func (m *Message) ToString() string {
	return fmt.Sprintf("%s#|#%s#|#%s#|#%s", m.Remote, m.Type, m.Error, m.Message)
}

func GetRemote(host, port string) string {
	return host + ":" + port
}

func ParseMessage(messageString string) *Message {
	fstSplit := strings.Split(messageString, "#|#")
	msg := Message{fstSplit[0], fstSplit[1], fstSplit[2], fstSplit[3]}
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
	message.Error = ""
	message.Message = msg
	con, err := net.Dial("tcp", toRemote)
	if err != nil {
		fmt.Printf("Host not found: %s\n", err)
		return
	}
	defer con.Close()
	fmt.Println("Dialing remote: " + toRemote)
	in, err := con.Write([]byte(message.ToString()))
	if err != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", err, in)
	}
}

func DialMessage(message Message, toRemote string) {
	con, err := net.Dial("tcp", toRemote)
	if err != nil {
		fmt.Printf("Host not found: %s\n", err)
		return
	}
	defer con.Close()
	fmt.Println("Dialing remote: " + toRemote)
	in, err := con.Write([]byte(message.ToString()))
	if err != nil {
		fmt.Printf("Error sending data: %s, in: %d\n", err, in)
	}
}
