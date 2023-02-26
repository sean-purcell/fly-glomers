package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func sendUntilSuccess(n *maelstrom.Node, peer string, message int) {
	body := map[string]any{}
	body["type"] = "broadcast"
	body["message"] = message
	for {
		_, err := n.SyncRPC(context.Background(), peer, body)
		if err != nil {
			log.Print("Failed to send RPC", err)
			time.Sleep(time.Second / 4)
		} else {
			break
		}
	}
}

func main() {
	var lock sync.Mutex
	n := maelstrom.NewNode()

	seen := map[int]struct{}{}
	peers := []string{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		val := int(body["message"].(float64))
		_, is_new := seen[val]

		if !is_new {
			seen[val] = struct{}{}
			for _, peer := range peers {
				go sendUntilSuccess(n, peer, val)
			}
		}
		lock.Unlock()

		seen[int(body["message"].(float64))] = struct{}{}
		delete(body, "message")
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		keys := make([]int, 0, len(seen))
		for k := range seen {
			keys = append(keys, k)
		}
		lock.Unlock()
		sort.Ints(keys)
		body["messages"] = keys
		body["type"] = "read_ok"

		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		whoami := n.ID()
		top := body["topology"]
		log.Print("received topology", top)
		new_peers := body["topology"].(map[string]any)[whoami].([]any)
		peers = []string{}
		for _, peer := range new_peers {
			peers = append(peers, peer.(string))
		}

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
