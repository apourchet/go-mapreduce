package mapreduce

import (
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
	trimmed := strings.TrimLeft(pair, "{")
	trimmed = strings.TrimRight(trimmed, "}")
	arr := strings.Split(trimmed, ",")
	return KVPair{arr[0], arr[1]}
}

func KVPairsToString(pairs []KVPair) string {
	str := "{"
	for _, pair := range pairs {
		str += pair.ToString() + ","
	}
	str += "}"
	return str
}

// TODO unmarshaling
func ParseKVPairs(pairs string) []KVPair {
	return []KVPair{{"1", "DEFAULT"}}
}
