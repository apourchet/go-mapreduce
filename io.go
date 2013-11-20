package mapreduce

import (
	"bufio"
	"bytes"
	"os"
)

func WriteFile(fileName, fileContent string) {
	output := bytes.NewBufferString(fileContent)
	sysfile, _ := os.Create(fileName)
	defer func() {
		if err := sysfile.Close(); err != nil {
			panic(err)
		}
	}()
	sysfile.Write(output.Bytes())
}

func ReadFile(fileName string) string {
	output := bytes.NewBufferString("")
	fi, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(fi)
	for buffer, _, err := reader.ReadLine(); err == nil; buffer, _, err = reader.ReadLine() {
		output.Write(buffer)
		output.WriteRune('\n')
	}
	return output.String()
}
