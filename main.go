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

type rideFare struct {
	RideID int
	Fare   float64
}

var (
	numWorkers = 6
	poolSize   = numWorkers * 2
	pool       = make(chan work, poolSize)
)

func main() {
	workChan := make(chan work, 10000) // probably procs num
	fareChan := make(chan *rideFare, 10000)

	go produceWork(getSource(), workChan)
	go spawnWorkers(workChan, fareChan)

	runCSVSink(getSink(), fareChan)
}

func getSource() io.ReadCloser {
	if len(os.Args) < 2 {
		return os.Stdin
	}

	in, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	return in
}

func getSink() io.WriteCloser {
	if len(os.Args) < 3 {
		return os.Stdout
	}

	out, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	return out
}

func produceWork(source io.ReadCloser, workChan chan work) {
	defer source.Close()
	defer close(workChan)

	reader := csv.NewReader(source)

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

	p1, _ := parsePath(w[0])

	for _, record := range w[1:] {
		p2, _ := parsePath(record)
		// if err not nil log error and skip

		duration := p2.Timestamp.Sub(p1.Timestamp).Hours()
		distance := Distance(Coord{p1.Lat, p1.Lng}, Coord{p2.Lat, p2.Lng})
		speed := distance / duration

		if speed > 100 {
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

func runCSVSink(sink io.WriteCloser, faresChan chan *rideFare) {
	defer sink.Close()

	writer := bufio.NewWriter(sink)
	defer writer.Flush()

	for fare := range faresChan {
		writer.WriteString(fmt.Sprintf("%d,%v\n", fare.RideID, fare.Fare))
	}
}
