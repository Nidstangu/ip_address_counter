package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

var addressBatchNameToFileMap map[string]*os.File
var linesRead int

func main() {
	// init map
	addressBatchNameToFileMap = make(map[string]*os.File)
	// open file
	file, err := os.Open("ip_addresses")
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	// read file
	reader := bufio.NewReader(file)
	for {
		// read line by line
		line, err := reader.ReadString('\n')
		linesRead++
		if err != nil {
			// handle last batch
			if err.Error() == "EOF" {
				index := ipIndex(line)
				if _, ok := addressBatchNameToFileMap[fmt.Sprintf("tmp/address_batch_%v", index)]; !ok {
					initializeBatch(index, line)
					continue
				}
				addToBatch(index, line)
				break
			}
			log.Fatalf("error reading file: %v", err)
		}
		index := ipIndex(line)
		if _, ok := addressBatchNameToFileMap[fmt.Sprintf("tmp/address_batch_%v", index)]; !ok {
			if _, ok := addressBatchNameToFileMap[fmt.Sprintf("tmp/address_batch_%v", index)]; !ok {
				initializeBatch(index, line)
				continue
			}
			addToBatch(index, line)
		}
		uniqueAddressCount := 0
		// count and clean up
		for k, v := range addressBatchNameToFileMap {
			count := counter(k)
			uniqueAddressCount += count
			v.Close()
			os.Remove(k)
		}
		fmt.Println("Unique Addresses:", uniqueAddressCount)
	}

	func ipIndex(ip string) string {
		strArr := strings.Split(ip, ".")
		return strArr[0]
	}

	func initializeBatch(index string, ip string) {
		batch, err := os.Create(fmt.Sprintf("tmp/address_batch_%v", index))
		if err != nil {
			fmt.Println(err)
		}
		addressBatchNameToFileMap[batch.Name()] = batch
		batch.WriteString(ip)
	}

	func addToBatch(index string, ip string) {
		batch := addressBatchNameToFileMap[fmt.Sprintf("tmp/address_batch_%v", index)]
		batch.WriteString(ip)
	}
