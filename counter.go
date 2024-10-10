package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type FourDimensionalMapWithMutex struct {
	IPMap map[byte]map[byte]map[byte]map[byte]struct{}
	mutex sync.Mutex
}

var mapWithMutex FourDimensionalMapWithMutex

const BatchSize = 5             // specify batch size for each routine
const MaxConcurrentRoutines = 8 // specify how many routines you want to spawnx

func counter(fileName string) int {
	// open file and defer closing
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	// read file
	reader := bufio.NewReader(file)
	// initialize global map
	mapWithMutex = FourDimensionalMapWithMutex{
		IPMap: make(map[byte]map[byte]map[byte]map[byte]struct{}),
	}
	waitChan := make(chan struct{}, MaxConcurrentRoutines)
	var wg sync.WaitGroup
	batch := make([]string, 0, BatchSize)
	for {
		// read lines one by one
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				// ensure last batch is not lost
				waitChan <- struct{}{}
				wg.Add(1)
				go process(batch, &wg, waitChan)
				break
			}
			log.Fatalf("error reading file: %v", err)
		}
		// append to batch
		batch = append(batch, line)
		// process when batch limit is reached, empty the batch
		if len(batch) >= BatchSize {
			waitChan <- struct{}{}
			wg.Add(1)
			go process(batch, &wg, waitChan)
			batch = make([]string, 0, BatchSize)
		}
	}
	// wait for all routines to finish working
	wg.Wait()
	var count int
	// iterate over the map and increment end nodes
	for _, v1 := range mapWithMutex.IPMap {
		for _, v2 := range v1 {
			for _, v3 := range v2 {
				for _, _ = range v3 {
					count++
				}
			}
		}
	}
	return count
}

// process splits addresses from the batch into bytes. this function does not handle inconsistent data in provided file
func process(addressBatch []string, wg *sync.WaitGroup, waitChan chan struct{}) {
	defer wg.Done()
	for _, address := range addressBatch {
		bytes := ipToBytes(address)
		mapIPAddress(bytes)
	}
	<-waitChan
}

// ipToBytes converts ip address provided as a string to [4]byte structure. this function does not handle inconsistent addresses
func ipToBytes(ip string) [4]byte {
	strArr := strings.Split(ip, ".")
	first, _ := strconv.Atoi(strArr[0])
	second, _ := strconv.Atoi(strArr[1])
	third, _ := strconv.Atoi(strArr[2])
	fourthSplit := strings.Split(strArr[3], "\n")
	fourth, _ := strconv.Atoi(fourthSplit[0])
	var ipBytes [4]byte
	ipBytes[0] = byte(first)
	ipBytes[1] = byte(second)
	ipBytes[2] = byte(third)
	ipBytes[3] = byte(fourth)
	return ipBytes
}

// mapIPAddress maps address to global map
func mapIPAddress(ip [4]byte) {
	mapWithMutex.mutex.Lock()
	defer mapWithMutex.mutex.Unlock()
	if _, ok := mapWithMutex.IPMap[ip[0]]; !ok {
		mapWithMutex.IPMap[ip[0]] = make(map[byte]map[byte]map[byte]struct{})
	}
	if _, ok := mapWithMutex.IPMap[ip[0]][ip[1]]; !ok {
		mapWithMutex.IPMap[ip[0]][ip[1]] = make(map[byte]map[byte]struct{})
	}
	if _, ok := mapWithMutex.IPMap[ip[0]][ip[1]][ip[2]]; !ok {
		mapWithMutex.IPMap[ip[0]][ip[1]][ip[2]] = make(map[byte]struct{})
	}
	mapWithMutex.IPMap[ip[0]][ip[1]][ip[2]][ip[3]] = struct{}{}
}
