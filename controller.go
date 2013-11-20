package mapreduce

import (
	"fmt"
	"strings"
	"time"
)

type Controller struct {
	Remote        string
	IdleWorkers   []string
	MapWorkers    []string
	ReduceWorkers []string

	UncompMaps    map[string]bool
	UncompReduces map[string]bool

	FinishedMaps    []KVPair
	FinishedReduces []KVPair
}

func NewController(serverRemote string) *Controller {
	a := make(map[string]bool)
	b := make(map[string]bool)
	return &Controller{serverRemote, []string{}, []string{}, []string{}, a, b, []KVPair{}, []KVPair{}}
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
	case MapperReady:
		c.HandleMapperReady(message)
	case ReducerReady:
		c.HandleReducerReady(message)
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

func (c *Controller) HandleMapperReady(message Message) {
	fmt.Println("Mapper is ready!")
	c.MapWorkers = append(c.MapWorkers, message.Remote)
}

func (c *Controller) HandleMapResults(message Message) {
	fmt.Println("Map Results have arrived!")
	if message.Error != "" {
		return
	}
	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0]
	delete(c.UncompMaps, jobId)

	pairs := ParseKVPairs(arr[1])
	for _, pair := range pairs {
		c.FinishedMaps = append(c.FinishedMaps, pair)
	}
}

func (c *Controller) HandleReducerReady(message Message) {
	fmt.Println("Reducer is Ready!")
}

func (c *Controller) HandleReduceResults(message Message) {
	fmt.Println("Reduce Results have arrived!")
	fmt.Println(message.Message)
}

// MapReduce starts here
func (c *Controller) Map(kvPairs []KVPair, mapJob string) []KVPair {
	fmt.Println("Mapping: " + mapJob)
	// Setup the uncompleted maps
	for _, pair := range kvPairs {
		c.UncompMaps[pair.Key] = true
	}

	results := []KVPair{}
	// For each task
	for _, workerRemote := range c.IdleWorkers {
		// Send a worker the map initialization job
		message := c.MapJobMessage(ReadFile(mapJob))
		DialMessage(message, workerRemote)
	}
	for len(c.MapWorkers) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for _, pair := range kvPairs {
		message := c.MapRunMessage(pair.Key, pair.Value)
		DialMessage(message, c.MapWorkers[0])
	}
	for len(c.UncompMaps) != 0 {
		// TODO Redistribute the unfinished jobs
		fmt.Println("Number of maps to go:", len(c.UncompMaps))
		time.Sleep(1 * time.Second)
	}
	return results
}

func (c *Controller) Reduce(kvsPairs map[string][]string, reduceJob string) []KVPair {
	fmt.Println("Reducing!")
	results := []KVPair{}
	// For each worker that is Idle
	for _, workerRemote := range c.IdleWorkers {
		// Send him the map initialization job
		message := c.ReduceJobMessage(reduceJob)
		DialMessage(message, workerRemote)
	}
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
	for len(c.IdleWorkers) == 0 {
		fmt.Println("Waiting for workers to connect...")
		time.Sleep(5 * time.Second)
	}
	mapped := c.Map(kvPairs, mapJob)
	combined := Combine(mapped)
	reduced := c.Reduce(combined, reduceJob)
	result := make(map[string]string)
	fmt.Println("Reduced Results:")
	for _, pair := range reduced {
		result[pair.Key] = pair.Value
		fmt.Println(pair.Key, ":", pair.Value)
	}
	return result
}
