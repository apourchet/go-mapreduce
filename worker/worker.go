package worker

import (
	. "../socketio"
)

type Worker struct {
	Remote   string
	Messages []Message
}
