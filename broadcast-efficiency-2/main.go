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

func sendUntilSuccess(n *maelstrom.Node, whoami string, peer string, messages []int) {
	delay := time.Second

	succeeded := false

	body := map[string]any{}
	body["type"] = "broadcast"
	body["messages"] = messages
	if whoami != "" {
		body["from"] = whoami
	}
	for {
		log.Printf("Going to send %v to %v", messages, peer)
		go func() {
			response, err := n.SyncRPC(context.Background(), peer, body)
			if err != nil {
				log.Print("Failed to send RPC", peer, messages, err)
			} else {
				succeeded = true
				log.Printf("RPC of %v to %v succeeded with %v", peer, messages, response)
			}
		}()

		time.Sleep(delay)
		if succeeded {
			break
		} else {
			log.Printf("Timed out or failed to send %v to %v", messages, peer)
		}
	}
}

func main() {
	var lock sync.Mutex
	n := maelstrom.NewNode()

	seen := map[int]struct{}{}
	dirty := false
	peers := []string{}

	var whoami string

	go func() {
		for {
			lock.Lock()
			is_dirty := dirty
			keys := make([]int, 0, len(seen))
			for k := range seen {
				keys = append(keys, k)
			}
			lock.Unlock()
			if is_dirty {
				for _, peer := range peers {
					go sendUntilSuccess(n, whoami, peer, keys)
				}
			}

			time.Sleep(time.Second / 8)
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		fval, has := body["message"]
		if has {
			val := int(fval.(float64))
			_, exists := seen[val]
			if !exists {
				seen[val] = struct{}{}
				dirty = true
			}
		}

		vals, has := body["messages"].([]any)
		if has {
			for _, v := range vals {
				val := int(v.(float64))
				_, exists := seen[val]
				if !exists {
					seen[val] = struct{}{}
					dirty = true
				}
			}
		}

		lock.Unlock()
		delete(body, "message")
		delete(body, "messages")
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
		dirty = true

		delete(body, "topology")
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
