package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/trace"
	"strconv"
	"sync"
	"time"
)

type pathNode struct {
	RideID    int
	Lat       float64
	Lng       float64
	Timestamp time.Time
}

type work [][]string

type rideFare struct {
	RideID int
	Fare   float64
}

func main() {
	// f, err := os.Create("./cpuprofile.out")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer f.Close()
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()

	f, err := os.Create("./trace.out")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := trace.Start(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer trace.Stop()

	///

	workChan := make(chan work, runtime.NumCPU()) // probably procs num
	fareChan := make(chan *rideFare, 10000)

	go runPathSource(workChan)
	go spawnWorkers(workChan, fareChan)

	runCSVSink(fareChan)
}

func spawnWorkers(workChan chan work, sink chan *rideFare) {
	defer close(sink)

	n := runtime.NumCPU() - 1
	// n := 1

	var wg sync.WaitGroup

	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			for w := range workChan {
				fare := 1.30
				p1, _ := parseRecord(w[0]) // Rename to Paths

				for _, record := range w[1:] {
					p2, _ := parseRecord(record)
					// if err not nil log error and skip

					t := p2.Timestamp.Sub(p1.Timestamp).Hours()
					s := Distance(Coord{p1.Lat, p1.Lng}, Coord{p2.Lat, p2.Lng})
					v := s / t

					if v > 100 {
						// Node is invalid, skip it and fetch next one
						continue
					}

					fare += getFare(s, v, t, p1, p2)

					p1 = p2
				}

				totalFare := math.Ceil(fare*100) / 100

				if totalFare < 3.47 {
					totalFare = 3.47
				}

				sink <- &rideFare{p1.RideID, totalFare}
				pool <- w
			}
		}()
	}

	wg.Wait()
}

func parseRecord(record []string) (*pathNode, error) {
	// TODO - Handle errors
	id, _ := strconv.Atoi(record[0])
	lat, _ := strconv.ParseFloat(record[1], 54)
	lng, _ := strconv.ParseFloat(record[2], 54)
	sec, _ := strconv.ParseInt(record[3], 10, 64)

	return &pathNode{
		RideID:    id,
		Lat:       lat,
		Lng:       lng,
		Timestamp: time.Unix(sec, 0),
	}, nil
}

func getFare(kms, speed, hours float64, p1, p2 *pathNode) float64 {
	if speed <= 10 {
		return 11.90 * hours
	}

	return 0.47
}

func runCSVSink(faresChan chan *rideFare) {
	out, err := os.Create("fares01.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	for fare := range faresChan {
		writer.WriteString(fmt.Sprintf("%d,%v\n", fare.RideID, fare.Fare))
	}
}

func runPathSource(workChan chan work) {
	defer close(workChan)

	file, _ := os.Open("./paths-large.csv")
	// file, _ := os.Open("./xxl.csv")
	defer file.Close()

	reader := csv.NewReader(file)

	// reader.ReuseRecord = true

	currentRideID := ""
	w := <-pool
	w = w[:0]

	// TODO - Reuse whole work struct ?

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				// TODO - Is this the last record
				return
			}
			log.Fatal(err)
		}

		if currentRideID == "" {
			currentRideID = record[0]
		}

		if record[0] != currentRideID {
			workChan <- w

			currentRideID = record[0]
			// nodes = [][]string{}
			w = <-pool
			w = w[:0]

			continue
		}

		w = append(w, record)
	}
}

var pool = make(chan work, runtime.NumCPU()*2)

func init() {
	for i := 0; i < runtime.NumCPU()*2; i++ {
		pool <- make(work, 0, 100)
	}
}
