package client

import (
	"fmt"
	"strconv"
	"sync"
)

type LamportTs struct {
	mutex       sync.Mutex
	id          int
	lamportTime int64
}

// NewLamportManager creates a new process with a given ID.
func NewLamportManager(idStr string) *LamportTs {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(err.Error())
	}
	return &LamportTs{
		id: id,
	}
}

// Event increments the process's Lamport timestamp for a local event.
func (p *LamportTs) Event() int64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.lamportTime++
	fmt.Printf("LamportTs %d experienced an event, timestamp: %d\n", p.id, p.lamportTime)
	return p.lamportTime
}

func (p *LamportTs) SendMessage(client *RabbitMQClient, msg []byte) (int64, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.lamportTime++
	err := client.sendMsg(msg)
	if err != nil {
		return 0, err
	}
	return p.lamportTime, nil
}

// ReceiveMessage simulates receiving a message from another process.
// It updates the Lamport timestamp based on the timestamp received with the message.
func (p *LamportTs) ReceiveMessage(timestamp int64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if timestamp > p.lamportTime {
		p.lamportTime = timestamp
	}
	p.lamportTime++
	fmt.Printf("LamportTs %d received a message, updated timestamp: %d\n", p.id, p.lamportTime)
}
