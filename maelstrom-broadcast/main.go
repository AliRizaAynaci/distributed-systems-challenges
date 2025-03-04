package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastingNode struct {
	node      *maelstrom.Node
	mu        sync.Mutex
	messages  map[int]struct{}
	neighbors map[string]struct{}
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
	return bn.node.Run()
}

func (bn *BroadcastingNode) Reply(msg maelstrom.Message, body any) error {
	return bn.node.Reply(msg, body)
}

func (bn *BroadcastingNode) ID() string {
	return bn.node.ID()
}

func (bn *BroadcastingNode) Read(msg maelstrom.Message) error {
	bn.mu.Lock()
	result := make([]int, 0, len(bn.messages))

	for k := range bn.messages {
		result = append(result, k)
	}
	bn.mu.Unlock()
	var response map[string]any = make(map[string]any)
	response["type"] = "read_ok"
	response["messages"] = result

	bn.Reply(msg, response)
	return nil
}

func (bn *BroadcastingNode) Broadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	bn.mu.Lock()
	value, ok := body["message"].(float64)
	if !ok {
		bn.mu.Unlock()
		return nil
	}

	messageValue := int(value)
	bn.messages[messageValue] = struct{}{}
	bn.mu.Unlock()

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
	bn.mu.Lock()
	bn.neighbors = result
	bn.mu.Unlock()
	bn.Reply(msg, map[string]string{"type": "topology_ok"})
	return nil
}

func main() {
	bn := NewBroadcastingBode()
	if err := bn.Run(); err != nil {
		log.Fatal("Running error")
	}
}

// ./maelstrom test -w unique-ids --bin "/mnt/c/Users/aynac/go/bin/unique-ids.exe" --node-count 3 --time-limit 30 --rate 1000 00--availability total
// --nemesis partition

// ./maelstrom test -w broadcast --bin "/mnt/c/Users/aynac/go/bin/maelstrom-broadcast.exe" --node-count 1 --time-limit 20 --rate 10
