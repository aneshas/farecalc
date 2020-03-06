package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestProduceWork_Produces_Work_For_RideID(t *testing.T) {
	segmentCount := 5
	paths := `1,37.966660,23.728308,1405594957
1,37.966627,23.728263,1405594966
1,37.966625,23.728263,1405594974
1,37.966613,23.728375,1405594984
1,37.966203,23.728597,1405594992
`
	wc := make(chan work, 1)

	produceWork(strings.NewReader(paths), wc)

	w := <-wc

	if len(w) != segmentCount {
		t.Fatalf("invalid number of segments! want: %d got: %d", segmentCount, len(w))
	}
}

func TestProduceWork_Produces_WorkBatch_For_Multiple_RideIDs(t *testing.T) {
	rideCount := 3
	paths := `1,37.966660,23.728308,1405594957
1,37.966627,23.728263,1405594966
1,37.966625,23.728263,1405594974
1,37.966613,23.728375,1405594984
1,37.966203,23.728597,1405594992
2,37.966660,23.728308,1405594957
2,37.966627,23.728263,1405594966
2,37.966625,23.728263,1405594974
2,37.966613,23.728375,1405594984
3,37.966660,23.728308,1405594957
3,37.966627,23.728263,1405594966
3,37.966625,23.728263,1405594974
3,37.966613,23.728375,1405594984
`
	wc := make(chan work, rideCount)

	produceWork(strings.NewReader(paths), wc)

	if len(wc) != rideCount {
		t.Fatalf("invalid number of rides! want: %d got: %d", rideCount, len(wc))
	}
}

func TestParsePath_Parses_Path_From_CSV_Record(t *testing.T) {
	cases := []struct {
		rideID int
		record []string
		want   pathNode
	}{
		{
			rideID: 1,
			record: []string{"1", "37.966625", "23.728263", "1405594974"},
			want: pathNode{
				RideID:    1,
				Lat:       37.966625,
				Lng:       23.728263,
				Timestamp: getTimestamp("2014-07-17T11:02:54+00:00"),
			},
		},
		{
			rideID: 2,
			record: []string{"2", "37.966624", "23.728263", "1405594974"},
			want: pathNode{
				RideID:    2,
				Lat:       37.966624,
				Lng:       23.728263,
				Timestamp: getTimestamp("2014-07-17T11:02:54+00:00"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("ride_%d", tc.rideID), func(t *testing.T) {
			if got := parsePath(tc.record); !reflect.DeepEqual(&tc.want, got) {
				t.Fatalf("could not parse csv record! want: %v got: %v", &tc.want, got)
			}
		})
	}
}

func getTimestamp(str string) time.Time {
	t, _ := time.Parse(time.RFC3339, str)
	return t.UTC()
}

func TestRun(t *testing.T) {
	src, err := os.Open("./testdata/paths.csv")
	if err != nil {
		t.Fatal(err)
	}

	defer src.Close()

	var sink bytes.Buffer

	run(src, &sink)

	want, err := ioutil.ReadFile("./testdata/fares.csv")
	if err != nil {
		t.Fatal(err)
	}

	if !areEqual(want, sink.Bytes()) {
		t.Fatalf("invalid output! want: %s got: %s", string(want), sink.String())
	}
}

func areEqual(want, got []byte) bool {
	wslice := strings.Split(string(want), "\n")
	sort.Strings(wslice)

	gslice := strings.Split(string(got), "\n")
	sort.Strings(gslice)

	return reflect.DeepEqual(wslice, gslice)
}
