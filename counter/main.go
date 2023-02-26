package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Data struct {
	op  int
	val int
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			key_data, err := kv.Read(ctx, "counter")
			log.Printf("read from kv: %v", key_data)
			var prev_op int
			var prev_val int
			if err != nil {
				prev_op = 0
				prev_val = 0
			} else {
				prev_op = int(key_data.(map[string]interface{})["op"].(float64))
				prev_val = int(key_data.(map[string]interface{})["val"].(float64))
			}

			op := prev_op + 1
			val := prev_val + int(body["delta"].(float64))
			new_val := map[string]int{}
			new_val["op"] = op
			new_val["val"] = val
			log.Printf("Trying to update from %v to %v", key_data, new_val)
			err = kv.CompareAndSwap(ctx, "counter", key_data, new_val, true)
			if err == nil {
				log.Printf("Succeeded at updating %v to %v", key_data, new_val)
				break
			} else {
				log.Printf("Failed at updating %v to %v", key_data, new_val)
			}
		}

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var result int

		for {
			key_data, err := kv.Read(ctx, "counter")
			log.Printf("read from kv: %v", key_data)
			var prev_op int
			var prev_val int
			if err != nil {
				prev_op = 0
				prev_val = 0
			} else {
				prev_op = int(key_data.(map[string]interface{})["op"].(float64))
				prev_val = int(key_data.(map[string]interface{})["val"].(float64))
			}

			op := prev_op + 1
			val := prev_val
			new_val := map[string]int{}
			new_val["op"] = op
			new_val["val"] = val
			log.Printf("Trying to update from %v to %v", key_data, new_val)
			err = kv.CompareAndSwap(ctx, "counter", key_data, new_val, true)
			if err == nil {
				log.Printf("Succeeded at updating %v to %v", key_data, new_val)
				result = val
				break
			} else {
				log.Printf("Failed at updating %v to %v", key_data, new_val)
			}
		}

		body["type"] = "read_ok"
		body["value"] = result

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
