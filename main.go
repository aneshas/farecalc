package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

type pathNode struct {
	RideID    int
	Lat       float64
	Lng       float64
	Timestamp time.Time
}

type segment struct {
	RideID int
	Fare   float64 // decimal?
}

const queueSize = 512

func main() {
	unfilteredNodes := make(chan *pathNode, queueSize)
	segmentsWithFare := make(chan *segment, queueSize)

	go runNodeSource(unfilteredNodes)
	go calculateFare(unfilteredNodes, segmentsWithFare)

	aggregateSegments(segmentsWithFare)

	///

	// go runNodeSource(unfilteredNodes)

	// var nodes []*pathNode

	// for node := range unfilteredNodes {
	// 	nodes = append(nodes, node)
	// }

	// out, err := os.Create("paths-large.csv")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// defer out.Close()

	// num := 17773 // ~1GB

	// writer := bufio.NewWriter(out)

	// for i := 0; i < num; i++ {
	// 	for _, n := range nodes {
	// 		writer.WriteString(fmt.Sprintf("%d,%v,%v,%d\n", n.RideID+i*9, n.Lat, n.Lng, n.Timestamp.Unix()))
	// 	}
	// }
}

func calculateFare(source chan *pathNode, sink chan *segment) {
	defer close(sink)

	p1 := <-source

	for p2 := range source {
		if p2.RideID != p1.RideID {
			// This node indicates start of a new ride
			p1 = p2
			continue
		}

		t := p2.Timestamp.Sub(p1.Timestamp).Hours()
		s := Distance(Coord{p1.Lat, p1.Lng}, Coord{p2.Lat, p2.Lng})
		v := s / t

		if v > 100 {
			// Node is invalid, skip it and fetch next one
			continue
		}

		// Both nodes are valid

		sink <- &segment{
			RideID: p1.RideID,
			Fare:   getFare(s, v, t, p1, p2),
		}

		// Set start of a new node
		p1 = p2
	}
}

func getFare(kms, speed, hours float64, p1, p2 *pathNode) float64 {
	if speed <= 10 {
		return 11.90 * hours
	}

	return 0.47
}

func aggregateSegments(source chan *segment) {
	out, err := os.Create("fares.csv")
	if err != nil {
		log.Fatal(err)
	}

	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	rideID := -1
	var fare float64

	for ps := range source {
		if rideID == -1 {
			rideID = ps.RideID
		}

		if rideID != ps.RideID {
			totalFare := fare + 1.30
			totalFare = math.Ceil(totalFare*100) / 100

			if totalFare < 3.47 {
				totalFare = 3.47
			}

			writer.WriteString(fmt.Sprintf("%d,%v\n", rideID, totalFare))

			rideID = ps.RideID
			fare = 0
		}

		fare += ps.Fare
	}
}

func runNodeSource(sink chan *pathNode) {
	defer close(sink)

	file, _ := os.Open("./paths-large.csv")
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	for {
		node, err := parsePathNode(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatal(err)
		}

		sink <- node
	}
}

func parsePathNode(reader *csv.Reader) (*pathNode, error) {
	record, err := reader.Read()
	if err != nil {
		return nil, err
	}

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
