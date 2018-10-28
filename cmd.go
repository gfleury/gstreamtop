package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/gfleury/gstreamtop/tablestream"
)

func main() {
	_ = tablestream.CreateTable("salve")
	counter := regexp.MustCompile("(?P<sum1>[0-9]+):.*:(?P<aggregator>.[^:]*)$")
	scanner := bufio.NewScanner(os.Stdin)
	memoryTable := make(map[string]map[string]interface{})
	for scanner.Scan() {
		aggregatorIndex := ""

	}
	fmt.Println(memoryTable)
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}
