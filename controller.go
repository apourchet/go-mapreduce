package mapreduce

import (
	"fmt"
	"strings"
	. "sync"
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

	mapMutex    Mutex
	reduceMutex Mutex
}

func NewController(serverRemote string) *Controller {
	a := make(map[string]bool)
	b := make(map[string]bool)
	return &Controller{serverRemote, []string{}, []string{}, []string{}, a, b, []KVPair{}, []KVPair{}, Mutex{}, Mutex{}}
}

func (c *Controller) HandleMapResults(message Message) {
	if message.Error != "" {
		return
	}
	fmt.Println("Map Results have arrived!")
	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0]
	c.RemoveUnfinishedMap(jobId)

	pairs := ParseKVPairs(arr[1])
	c.AddFinishedMap(pairs)
}

func (c *Controller) HandleReduceResults(message Message) {
	if message.Error != "" {
		return
	}
	fmt.Println("Reduce Results have arrived!")
	arr := strings.Split(message.Message, ARGSEP)
	jobId := arr[0]
	c.RemoveUnfinishedReduce(jobId)

	pair := ParseKVPair(arr[1])
	c.AddFinishedReduce(pair)
}

// MapReduce starts here
func (c *Controller) Map(kvPairs []KVPair, mapJob string) []KVPair {
	fmt.Println("Mapping: " + mapJob)
	for _, pair := range kvPairs {
		c.UncompMaps[pair.Key] = true
	}
	for _, workerRemote := range c.IdleWorkers {
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
	return c.FinishedMaps
}

func (c *Controller) Reduce(kvsPairs map[string][]string, reduceJob string) []KVPair {
	fmt.Println("Reducing!")
	for key, _ := range kvsPairs {
		c.UncompReduces[key] = true
	}
	for _, workerRemote := range c.IdleWorkers {
		message := c.ReduceJobMessage(ReadFile(reduceJob))
		DialMessage(message, workerRemote)
	}
	for len(c.ReduceWorkers) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for key, vs := range kvsPairs {
		message := c.ReduceRunMessage(key, fmt.Sprintf("%s", vs))
		DialMessage(message, c.ReduceWorkers[0])
	}
	for len(c.UncompReduces) != 0 {
		// TODO Redistribute the unfinished jobs
		fmt.Println("Number of reduces to go:", len(c.UncompReduces))
		time.Sleep(1 * time.Second)
	}
	return c.FinishedReduces
}

// Combines the Key-Value pairs with the same keys to form an array of values
func Combine(kvPairs []KVPair) map[string][]string {
	fmt.Println("Combining!")
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
	c.WaitForWorkers()

	mapped := c.Map(kvPairs, mapJob)
	PrintMapResults(mapped)
	combined := Combine(mapped)
	PrintCombineResults(combined)
	reduced := c.Reduce(combined, reduceJob)
	PrintReduceResults(reduced)

	result := make(map[string]string)
	for _, pair := range reduced {
		result[pair.Key] = pair.Value
	}

	return result
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

func (c *Controller) WaitForWorkers() {
	for len(c.IdleWorkers) == 0 {
		fmt.Println("Waiting for workers to connect...")
		time.Sleep(5 * time.Second)
	}
}

func (c *Controller) RemoveUnfinishedMap(key string) {
	c.mapMutex.Lock()
	delete(c.UncompMaps, key)
	c.mapMutex.Unlock()
}

func (c *Controller) AddFinishedMap(pairs []KVPair) {
	c.mapMutex.Lock()
	for _, pair := range pairs {
		c.FinishedMaps = append(c.FinishedMaps, pair)
	}
	c.mapMutex.Unlock()
}

func (c *Controller) RemoveUnfinishedReduce(key string) {
	c.reduceMutex.Lock()
	delete(c.UncompReduces, key)
	c.reduceMutex.Unlock()
}

func (c *Controller) AddFinishedReduce(pair KVPair) {
	c.reduceMutex.Lock()
	c.FinishedReduces = append(c.FinishedReduces, pair)
	c.reduceMutex.Unlock()
}

func (c *Controller) HandleNewWorker(message Message) {
	fmt.Println("Worker is ready on remote: " + message.Remote)
	c.IdleWorkers = append(c.IdleWorkers, message.Remote)
}

func (c *Controller) HandleMapperReady(message Message) {
	fmt.Println("Mapper is ready!")
	c.MapWorkers = append(c.MapWorkers, message.Remote)
}

func (c *Controller) HandleReducerReady(message Message) {
	fmt.Println("Reducer is Ready!")
	c.ReduceWorkers = append(c.ReduceWorkers, message.Remote)
}

func PrintMapResults(mapped []KVPair) {
	fmt.Println("Map results: ")
	for _, pair := range mapped {
		fmt.Println(pair.ToString())
	}
	fmt.Println()
}

func PrintReduceResults(reduced []KVPair) {
	fmt.Println("Reduce results: ")
	for _, pair := range reduced {
		fmt.Println(pair.ToString())
	}
	fmt.Println()
}

func PrintCombineResults(combined map[string][]string) {
	fmt.Println("Combine results: ")
	for key, values := range combined {
		fmt.Println(key+" ->", values)
	}
	fmt.Println()
}
