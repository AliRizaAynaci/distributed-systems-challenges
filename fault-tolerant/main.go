package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastingNode struct {
	node               *maelstrom.Node
	messagesLock       sync.Mutex
	messages           map[int]struct{}
	toSendMessagesLock sync.Mutex
	toSendMessages     map[int]map[string]struct{}
	neighbors          map[string]struct{}
}

func NewBroadcastingBode() *BroadcastingNode {
	n := maelstrom.NewNode()
	bn := &BroadcastingNode{
		node:      n,
		messages:  make(map[int]struct{}),
		neighbors: make(map[string]struct{}),
	}

	n.Handle("read", bn.Read)
	n.Handle("broadcast", bn.Broadcast)
	n.Handle("topology", bn.Topology)
	return bn
}

func (bn *BroadcastingNode) Run() error {
	go bn.Retry()
	return bn.node.Run()
}

func (bn *BroadcastingNode) Reply(msg maelstrom.Message, body any) error {
	return bn.node.Reply(msg, body)
}

func (bn *BroadcastingNode) ID() string {
	return bn.node.ID()
}

func (bn *BroadcastingNode) Read(msg maelstrom.Message) error {
	bn.messagesLock.Lock()
	result := make([]int, 0, len(bn.messages))

	for k := range bn.messages {
		result = append(result, k)
	}
	bn.messagesLock.Unlock()
	var response map[string]any = make(map[string]any)
	response["type"] = "read_ok"
	response["messages"] = result

	bn.Reply(msg, response)
	return nil
}

type BroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func (bn *BroadcastingNode) Broadcast(msg maelstrom.Message) error {
	var body BroadcastMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	bn.messagesLock.Lock()
	if _, exists := bn.messages[body.Message]; exists {
		bn.messagesLock.Unlock()
		return nil
	}

	bn.messages[body.Message] = struct{}{}
	bn.messagesLock.Unlock()

	bn.toSendMessagesLock.Lock()
	if _, exists := bn.toSendMessages[body.Message]; !exists {
		bn.toSendMessages[body.Message] = make(map[string]struct{})
	}

	for neighbor := range bn.neighbors {
		bn.toSendMessages[body.Message][neighbor] = struct{}{}
	}
	bn.toSendMessagesLock.Unlock()

	for neighbor := range bn.neighbors {
		bn.SendRPC(neighbor, body)
	}

	var response map[string]any = make(map[string]any)
	response["type"] = "broadcast_ok"
	bn.Reply(msg, response)
	return nil
}

type TopologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (bn *BroadcastingNode) Topology(msg maelstrom.Message) error {
	var body TopologyMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	result := make(map[string]struct{})
	nodeId := bn.ID()
	if neighbors, exists := body.Topology[nodeId]; exists {
		for _, neighbor := range neighbors {
			result[neighbor] = struct{}{}
		}
	}
	bn.messagesLock.Lock()
	bn.neighbors = result
	bn.messagesLock.Unlock()
	bn.Reply(msg, map[string]string{"type": "topology_ok"})
	return nil
}

func (bn *BroadcastingNode) SendRPC(neighbor string, body BroadcastMessage) {
	bn.node.RPC(neighbor, body, func(msg maelstrom.Message) error {
		var response map[string]any
		if err := json.Unmarshal(msg.Body, &response); err != nil {
			return err
		}

		if response["type"] != "broadcast_ok" {
			return nil
		}

		bn.toSendMessagesLock.Lock()
		if _, exists := bn.toSendMessages[body.Message]; exists {
			delete(bn.toSendMessages[body.Message], neighbor)
		}
		bn.toSendMessagesLock.Unlock()
		return nil
	})
}

func (bn *BroadcastingNode) Retry() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		bn.toSendMessagesLock.Lock()
		var toSend = bn.toSendMessages
		bn.toSendMessagesLock.Unlock()
		for message, neighbors := range toSend {
			for neighbor := range neighbors {
				var msg BroadcastMessage = BroadcastMessage{
					Type:    "broadcast",
					Message: message,
				}
				bn.SendRPC(neighbor, msg)
			}
		}
	}
}

func main() {
	bn := NewBroadcastingBode()
	if err := bn.Run(); err != nil {
		log.Fatal("Running error")
	}
}

// FAULT_TOLERANT TEST COMMAND
// ./maelstrom test -w broadcast --bin "/mnt/c/Users/aynac/go/bin/fault-tolerant.exe" --node-count 5 --time-limit 20 --rate 10 --nemesis partition
