package main

import (
	"fmt"
	"go-raft/pkg/raft"
	"log"
	"os"
	"strings"
)

// dump raft log file
func main() {
	if len(os.Args) != 2 {
		fmt.Println(("Usage: logdump logfilename"))
		os.Exit(-1)
	}
	logstore, err := raft.NewLogStore(os.Args[1], log.Default())
	if err != nil {
		log.Fatal("cannot open file", err)
	}
	logs := logstore.Entries(0, logstore.TotalCount())
	for _, log := range logs {
		fmt.Printf("{\"Index\":%d, \"Term\":%d, \"Payload\":\"%s\"}\n", log.LogIndex, log.Term, log.Payload)
	}
}

func dumpBytes(bytes []byte) string {
	var sb strings.Builder
	for _, b := range bytes {
		sb.WriteString(fmt.Sprintf("%d", b))
	}
	return sb.String()
}
