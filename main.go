package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
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

var numWorkers = runtime.NumCPU()

func main() {
	src := getSource()
	defer src.Close()

	sink := getSink()
	defer sink.Close()

	run(src, sink)
}

func run(src io.Reader, sink io.Writer) {
	workChan := make(chan work, numWorkers)
	fareChan := make(chan *rideFare, numWorkers)

	go produceWork(src, workChan)
	go spawnWorkers(workChan, fareChan)

	runCSVSink(sink, fareChan)
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

func produceWork(source io.Reader, workChan chan work) {
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

	var p1, p2 *pathNode

	p1 = parsePath(w[0])

	for _, record := range w[1:] {
		p2 = parsePath(record)

		seg := segment{
			DurationH:  p2.Timestamp.Sub(p1.Timestamp).Hours(),
			DistanceKM: Distance(Coord{p1.Lat, p1.Lng}, Coord{p2.Lat, p2.Lng}),
			TimeOfDay:  p1.Timestamp,
		}

		seg.SpeedKMH = seg.DistanceKM / seg.DurationH

		if seg.SpeedKMH > 100 {
			continue
		}

		fare += getSegmentFare(&seg)

		p1 = p2
	}

	return &rideFare{p1.RideID, getRideFare(fare)}
}

func parsePath(record []string) *pathNode {
	id, err := strconv.Atoi(record[0])
	if err != nil {
		log.Fatal(err)
	}

	lat, err := strconv.ParseFloat(record[1], 54)
	if err != nil {
		log.Fatal(err)
	}

	lng, err := strconv.ParseFloat(record[2], 54)
	if err != nil {
		log.Fatal(err)
	}

	sec, err := strconv.ParseInt(record[3], 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	return &pathNode{
		RideID:    id,
		Lat:       lat,
		Lng:       lng,
		Timestamp: time.Unix(sec, 0).UTC(),
	}
}

func runCSVSink(sink io.Writer, faresChan chan *rideFare) {
	writer := bufio.NewWriter(sink)
	defer writer.Flush()

	for fare := range faresChan {
		writer.WriteString(fmt.Sprintf("%d,%v\n", fare.RideID, fare.Fare))
	}
}
