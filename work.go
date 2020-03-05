package main

type work [][]string

var (
	poolSize = numWorkers * 2
	pool     = make(chan work, poolSize)
)

func init() {
	for i := 0; i < poolSize; i++ {
		pool <- make(work, 0, 100)
	}
}

func getWork() work {
	w := <-pool
	return w[:0]
}
