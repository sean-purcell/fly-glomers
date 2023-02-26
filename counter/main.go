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
			log.Printf("Trying to update from %v,%v to %v,%v", prev_op, prev_val, op, val)
			err = kv.CompareAndSwap(ctx, "counter", Data{op: prev_op, val: prev_val}, Data{op, val}, true)
			if err == nil {
				log.Printf("Succeeded at updating from %v,%v to %v,%v", prev_op, prev_val, op, val)
				break
			} else {
				log.Printf("Failed at updating from %v,%v to %v,%v", prev_op, prev_val, op, val)
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
			log.Printf("Trying to update from %v,%v to %v,%v", prev_op, prev_val, op, val)
			err = kv.CompareAndSwap(ctx, "counter", Data{op: prev_op, val: prev_val}, Data{op, val}, true)
			if err == nil {
				log.Printf("Succeeded at updating from %v,%v to %v,%v", prev_op, prev_val, op, val)
				result = val
				break
			} else {
				log.Printf("Failed at updating from %v,%v to %v,%v", prev_op, prev_val, op, val)
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
