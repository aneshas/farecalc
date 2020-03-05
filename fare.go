package main

import (
	"math"
	"time"
)

const (
	// FixedIdleFare represents fixed fare charge for idle time
	FixedIdleFare float64 = 11.90

	// EarlyRideFare is charged per kilometer during (00:00, 05:00]
	EarlyRideFare float64 = 1.30

	// NormalRideFare is charged per kilometer during (05:00, 00:00]
	NormalRideFare float64 = 0.74

	// IdleSpeedThresholdKMH represents threshold until which
	// a ride is being charged as idle
	IdleSpeedThresholdKMH = 10
)

var (
	// StandardFare is a standard flag fare
	StandardFare = 1.30

	// MinimumFare represents minimum ride fare
	MinimumFare = 3.47
)

var (
	midnight time.Time
	fiveAM   time.Time
)

func init() {
	midnight, _ = time.Parse("15:04:05", "00:00:00")
	fiveAM, _ = time.Parse("15:04:05", "05:00:01")
}

type segment struct {
	DistanceKM float64
	DurationH  float64
	SpeedKMH   float64
	TimeOfDay  time.Time
}

func getSegmentFare(seg *segment) float64 {
	if seg.SpeedKMH <= IdleSpeedThresholdKMH {
		return FixedIdleFare * seg.DurationH
	}

	t, _ := time.ParseInLocation(
		"15:04:05",
		seg.TimeOfDay.Format("15:04:05"),
		seg.TimeOfDay.Location(),
	)

	if t.After(midnight.In(t.Location())) && t.Before(fiveAM.In(t.Location())) {
		return EarlyRideFare * seg.DistanceKM
	}

	return NormalRideFare * seg.DistanceKM
}

func getRideFare(segmentsFare float64) float64 {
	segmentsFare += StandardFare

	if segmentsFare < MinimumFare {
		segmentsFare = MinimumFare
	}

	return round(segmentsFare)
}

func round(f float64) float64 {
	return math.Ceil(f*100) / 100
}
