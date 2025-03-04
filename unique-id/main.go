package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const timestamp = 1741048067000

type IdGenerator struct {
	lastEpoch      int64
	sequenceNumber int16
	mutex          sync.Mutex
}

func (idGen *IdGenerator) generate(nodeId int8) int64 {
	idGen.mutex.Lock()
	defer idGen.mutex.Unlock()

	epoch := time.Now().UnixMilli() - timestamp

	if epoch == idGen.lastEpoch {
		idGen.sequenceNumber = (idGen.sequenceNumber + 1) & 0x1FFF
		for epoch <= idGen.lastEpoch {
			epoch = time.Now().UnixMilli() - timestamp
		}
	} else {
		idGen.sequenceNumber = 0
	}

	idGen.lastEpoch = epoch

	return (idGen.lastEpoch << 22) | (int64(nodeId) << 14) | int64(idGen.sequenceNumber)
}

func getNodeIdAsInt(nodeIdStr string) int8 {
	intPart := nodeIdStr[1:]
	intId, err := strconv.Atoi(intPart)
	if err != nil {
		return 0
	}
	return int8(intId)
}

func main() {
	node := maelstrom.NewNode()
	idGen := &IdGenerator{}

	node.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["id"] = idGen.generate(getNodeIdAsInt(node.ID()))
		// body["id"] = uuid.New()
		body["type"] = "generate_ok"

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
