package mapreduce

import (
	// "fmt"
	"strings"
)

type KVPair struct {
	Key, Value string
}

type KVsPair struct {
	Key    string
	Values []string
}

// TODO marshaling
func (kvpair KVPair) ToString() string {
	return "{" + kvpair.Key + "," + kvpair.Value + "}"
}

// TODO unmarshaling
func ParseKVPair(pair string) KVPair {
	// fmt.Println("Parsing pair: " + pair)
	trimmed := strings.TrimLeft(pair, "{")
	trimmed = strings.TrimRight(trimmed, "}")
	arr := strings.Split(trimmed, ",")
	if len(arr) < 2 {
		return KVPair{"", ""}
	}
	return KVPair{arr[0], arr[1]}
}

func KVPairsToString(pairs []KVPair) string {
	str := "{"
	if len(pairs) == 0 {
		return "{}"
	}
	for _, pair := range pairs {
		str += pair.ToString() + ";"
	}
	str = str[:len(str)-1]
	str += "}"
	return str
}

// TODO unmarshaling
func ParseKVPairs(pairs string) []KVPair {
	// fmt.Println("Parsing pairs: " + pairs)
	trimmed := strings.TrimLeft(pairs, "{")
	trimmed = strings.TrimRight(trimmed, "}")
	arr := strings.Split(trimmed, ";")
	res := []KVPair{}
	for _, pair := range arr {
		kv := ParseKVPair(pair)
		res = append(res, kv)
	}
	return res
}
