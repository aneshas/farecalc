package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
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

var (
	numWorkers = 6
	poolSize   = numWorkers * 2
	pool       = make(chan work, poolSize)
)

func init() {
	for i := 0; i < poolSize; i++ {
		pool <- make(work, 0, 100)
	}
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

	// f, err := os.Create("./trace.out")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer f.Close()
	// if err := trace.Start(f); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer trace.Stop()

	///

	workChan := make(chan work, 10000) // probably procs num
	fareChan := make(chan *rideFare, 10000)

	go runPathSource(workChan)
	go spawnWorkers(workChan, fareChan)

	runCSVSink(fareChan)
}

func runPathSource(workChan chan work) {
	defer close(workChan)

	file, _ := os.Open("./paths-large.csv")
	defer file.Close()

	reader := csv.NewReader(file)

	var currentRideID *string
	w := getWork()

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				workChan <- w
				return
			}
			log.Fatal(err)
		}

		if currentRideID == nil {
			currentRideID = &record[0]
		}

		if record[0] != *currentRideID {
			workChan <- w

			currentRideID = &record[0]
			w = getWork()

			continue
		}

		w = append(w, record)
	}
}

func getWork() work {
	w := <-pool
	return w[:0]
}

func spawnWorkers(workChan chan work, sink chan *rideFare) {
	defer close(sink)

	var wg sync.WaitGroup

	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for w := range workChan {
				sink <- calcRideFare(w)
				pool <- w
			}
		}()
	}

	wg.Wait()
}

func calcRideFare(w work) *rideFare {
	var fare float64

	// TODO
	// 1. get segments
	// 2. filter segments
	// 3. accumulate fares

	p1, _ := parsePath(w[0])

	for _, record := range w[1:] {
		p2, _ := parsePath(record)
		// if err not nil log error and skip

		duration := p2.Timestamp.Sub(p1.Timestamp).Hours()
		distance := Distance(Coord{p1.Lat, p1.Lng}, Coord{p2.Lat, p2.Lng})
		speed := distance / duration

		if speed > 100 {
			// Node is invalid, skip it and fetch next one
			continue
		}

		fare += getSegmentFare(&Segment{
			DistanceKM: distance,
			DurationH:  duration,
			SpeedKMH:   speed,
			TimeOfDay:  p1.Timestamp,
		})

		p1 = p2
	}

	return &rideFare{p1.RideID, getRideFare(fare)}
}

func parsePath(record []string) (*pathNode, error) {
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
