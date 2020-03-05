package main

type work [][]string

func init() {
	for i := 0; i < poolSize; i++ {
		pool <- make(work, 0, 100)
	}
}

func getWork() work {
	w := <-pool
	return w[:0]
}
