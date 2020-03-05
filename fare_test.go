package main

import (
	"fmt"
	"testing"
	"time"
)

func TestGetsegmentFare_Charges_Fixed_Fee_For_Idle_Time(t *testing.T) {
	cases := []struct {
		speed    float64
		duration float64
		want     float64
	}{
		{
			speed:    0,
			duration: 1,
			want:     FixedIdleFare,
		},
		{
			speed:    10,
			duration: 2,
			want:     FixedIdleFare * 2,
		},
		{
			speed:    5,
			duration: 3,
			want:     FixedIdleFare * 3,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("speed: %v", tc.speed), func(t *testing.T) {
			fare := getSegmentFare(&segment{
				SpeedKMH:  tc.speed,
				DurationH: tc.duration,
			})

			if fare != tc.want {
				t.Fatalf("fixed fare not applied! want: %v got: %v", tc.want, fare)
			}
		})
	}
}

func TestGetsegmentFare_Charges_PerKM_Fee_For_Early_Ride(t *testing.T) {
	cases := []struct {
		speed     float64
		distance  float64
		timeOfDay time.Time
		want      float64
	}{
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("00:00:01"),
			want:      EarlyRideFare * 2,
		},
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("05:00:00"),
			want:      EarlyRideFare * 2,
		},
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("03:45:00"),
			want:      EarlyRideFare * 2,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("speed: %v", tc.speed), func(t *testing.T) {
			fare := getSegmentFare(&segment{
				SpeedKMH:   tc.speed,
				DistanceKM: tc.distance,
				TimeOfDay:  tc.timeOfDay,
			})

			if fare != tc.want {
				t.Fatalf("early fare not applied! want: %v got: %v", tc.want, fare)
			}
		})
	}
}

func TestGetsegmentFare_Charges_PerKM_Fee_For_Normal_Ride(t *testing.T) {
	cases := []struct {
		speed     float64
		distance  float64
		timeOfDay time.Time
		want      float64
	}{
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("05:00:01"),
			want:      NormalRideFare * 2,
		},
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("00:00:00"),
			want:      NormalRideFare * 2,
		},
		{
			speed:     11,
			distance:  2,
			timeOfDay: getTime("13:45:00"),
			want:      NormalRideFare * 2,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("speed: %v", tc.speed), func(t *testing.T) {
			fare := getSegmentFare(&segment{
				SpeedKMH:   tc.speed,
				DistanceKM: tc.distance,
				TimeOfDay:  tc.timeOfDay,
			})

			if fare != tc.want {
				t.Fatalf("early fare not applied! want: %v got: %v", tc.want, fare)
			}
		})
	}
}

func getTime(t string) time.Time {
	time, _ := time.Parse(time.RFC3339, fmt.Sprintf("2006-01-02T%sZ", t))
	return time
}

func TestGetFare(t *testing.T) {
	cases := []struct {
		name    string
		segFare float64
		want    float64
	}{
		{
			name:    "minimum fare applied",
			segFare: 0,
			want:    MinimumFare,
		},
		{
			name:    "standard fare applied",
			segFare: MinimumFare,
			want:    round(MinimumFare + StandardFare),
		},
		{
			name:    "standard fare and minimum applied to segments fare",
			segFare: 200.45,
			want:    round(200.45 + StandardFare),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			if fare := getRideFare(tc.segFare); fare != tc.want {
				t.Fatalf("incorrect fare! want: %v got: %v", tc.want, fare)
			}
		})
	}
}
