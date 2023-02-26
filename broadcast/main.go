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

func sendUntilSuccess(n *maelstrom.Node, whoami string, peer string, message int) {
	delay := time.Second

	succeeded := false

	body := map[string]any{}
	body["type"] = "broadcast"
	body["message"] = message
	if whoami != "" {
		body["from"] = whoami
	}
	for {
		log.Printf("Going to send %v to %v", message, peer)
		go func() {
			response, err := n.SyncRPC(context.Background(), peer, body)
			if err != nil {
				log.Print("Failed to send RPC", peer, message, err)
			} else {
				succeeded = true
				log.Printf("RPC of %v to %v succeeded with %v", peer, message, response)
			}
		}()

		time.Sleep(delay)
		if succeeded {
			break
		} else {
			log.Printf("Timed out or failed to send %v to %v", message, peer)
		}
	}
}

func main() {
	var lock sync.Mutex
	n := maelstrom.NewNode()

	seen := map[int]struct{}{}
	peers := []string{}

	var whoami string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := int(body["message"].(float64))
		from, has_from := body["from"]
		_ = from

		lock.Lock()
		_, is_new := seen[val]

		if !is_new {
			seen[val] = struct{}{}
			for _, peer := range peers {
				// ignoring topology so never gossip
				if has_from {
					// if peer == whoami || (has_from && from.(string) == peer) {

				} else {
					go sendUntilSuccess(n, whoami, peer, val)
				}
			}
		}

		seen[int(body["message"].(float64))] = struct{}{}

		lock.Unlock()
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

		whoami = n.ID()
		top := body["topology"]
		log.Print("received topology", top)
		new_peers := body["topology"].(map[string]any)[whoami].([]any)
		peers = []string{}
		for _, peer := range new_peers {
			peers = append(peers, peer.(string))
		}

		// Ignore the topology because the challenge makes it impossible
		peers = n.NodeIDs()

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
