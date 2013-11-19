package mapreduce

import (
	"fmt"
)

type KVPair struct {
	Key, Value string
}

type KVsPairs struct {
	Key    string
	Values []string
}

type Controller struct {
	Remote        string
	IdleWorkers   []string
	MapWorkers    []string
	ReduceWorkers []string
}

// Handles the message from a Worker after sending a job to it
func (c *Controller) HandleMessage(message Message) {
	// fmt.Println("Controller handling message:\n" + message.ToString())
	switch message.Type {
	case Test:
		fmt.Println("Handling Test message")
		response := TestMessage(c.Remote, "Response to test message")
		DialMessage(response, message.Remote)
	case WorkerReady:
		c.HandleNewWorker(message)
	case MapResult:
		c.HandleMapResults(message)
	case ReduceResult:
		c.HandleReduceResults(message)
	default:
		fmt.Println("Cannot handle that kind of message: " + message.Type)
	}
}

func (c *Controller) HandleNewWorker(message Message) {
	fmt.Println("Worker is ready on remote: " + message.Remote)
	c.IdleWorkers = append(c.IdleWorkers, message.Remote)
}

func (c *Controller) HandleMapResults(message Message) {
	fmt.Println("Map Results have arrived!")
}

func (c *Controller) HandleReduceResults(message Message) {
	fmt.Println("Reduce Results have arrived!")
	fmt.Println(message.Message)
}

func (c *Controller) Map(kvPairs []KVPair, mapJob string) []KVPair {
	results := []KVPair{}

	return results
}

func (c *Controller) Reduce(kvsPairs map[string][]string, reduceJob string) []KVPair {
	results := []KVPair{}

	return results
}

// Combines the Key-Value pairs with the same keys to form an array of values
func Combine(kvPairs []KVPair) map[string][]string {
	results := make(map[string][]string)
	for _, pair := range kvPairs {
		if val, presence := results[pair.Key]; presence {
			results[pair.Key] = append(val, pair.Value)
		} else {
			results[pair.Key] = []string{pair.Value}
		}
	}
	return results
}

func (c *Controller) MapReduce(kvPairs []KVPair, mapJob, reduceJob string) map[string]string {
	mapped := c.Map(kvPairs, mapJob)
	combined := Combine(mapped)
	reduced := c.Reduce(combined, reduceJob)
	result := make(map[string]string)
	for _, pair := range reduced {
		result[pair.Key] = pair.Value
	}
	return result
}
