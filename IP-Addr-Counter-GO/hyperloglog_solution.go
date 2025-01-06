package main

import (
	"bufio"
	"fmt"
	"github.com/schollz/progressbar/v3" // Progress bar for visualizing processing progress
	"hash/fnv"
	"log"
	"math"
	"math/bits"
	"os"
	"time"
)

const (
	p             = 12     // 2^p buckets
	m             = 1 << p // Total number of buckets (4096 for p=12)
	FILE_PATH     = "path/to/file"
	LOG_FILE_PATH = "hyperloglog_counter.log"
)

// https://en.wikipedia.org/wiki/HyperLogLog
type HyperLogLog struct {
	buckets []uint8
}

func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		buckets: make([]uint8, m),
	}
}

func setupLogger() *os.File {
	file, err := os.OpenFile(LOG_FILE_PATH, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	return file
}

func (hll *HyperLogLog) Add(data []byte) {
	h := fnv.New64a()
	h.Write(data)
	hash := h.Sum64()
	index := hash >> (64 - p)         // Extract the first p bits as bucket index
	value := hash<<p | (1 << (p - 1)) // Extract remaining bits as a value
	leftmostOnePosition := leadingZeros(value) + 1
	// Update the bucket if the new value is greater
	if hll.buckets[index] < leftmostOnePosition {
		hll.buckets[index] = leftmostOnePosition
	}
}

func (hll *HyperLogLog) Estimate() uint64 {
	sum := 0.0
	for _, v := range hll.buckets {
		sum += 1.0 / math.Pow(2.0, float64(v))
	}
	alphaMM := 0.7213 / (1 + 1.079/float64(m)) * float64(m*m)
	estimate := alphaMM / sum

	// Small cardinality correction
	if estimate <= 5.0/2.0*float64(m) {
		var zeros int
		// Count zero buckets
		for _, v := range hll.buckets {
			if v == 0 {
				zeros++
			}
		}
		// Apply correction if necessary
		if zeros > 0 {
			estimate = float64(m) * math.Log(float64(m)/float64(zeros))
		}
	} else if estimate > (1<<32)/30.0 { // Large cardinality correction
		estimate = -(1 << 32) * math.Log(1-estimate/(1<<32))
	}
	return uint64(estimate)
}

func leadingZeros(x uint64) uint8 {
	return uint8(bits.LeadingZeros64(x))
}

func processIPs() uint64 {
	startTime := time.Now()
	log.Printf("Starting HyperLogLog counting method at %v", startTime)

	hll := NewHyperLogLog()

	file, err := os.Open(FILE_PATH)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	bar := progressbar.Default(fileInfo.Size())

	for scanner.Scan() {
		line := scanner.Text()
		bar.Add64(int64(len(line) + 1)) // +1 for newline
		hll.Add([]byte(line))
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	estimate := hll.Estimate()
	duration := time.Since(startTime)
	log.Printf("HyperLogLog method finished. Duration: %v, Estimated Unique IPs: %d", duration, estimate)
	return estimate
}

func main() {
	logFile := setupLogger()
	defer logFile.Close()

	log.Println("Starting HyperLogLog IP counting")
	fmt.Println("Running HyperLogLog method...")

	estimate := processIPs()

	fmt.Printf("\nHyperLogLog result: %d estimated unique IPs\n", estimate)
	log.Printf("Program finished. Estimated %d unique IPs", estimate)
}
