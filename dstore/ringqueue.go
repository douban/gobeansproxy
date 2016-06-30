package dstore

import (
	"errors"
)

type RingQueue struct {
	data []float64
	head int
	tail int
}

var (
	ErrQueueFull  = errors.New("queue full")
	ErrQueueEmpty = errors.New("queue empty")
)

func NewRingQueue(cap int) *RingQueue {
	return &RingQueue{
		data: make([]float64, cap),
	}
}

func (q *RingQueue) Push(x float64) error {
	if (cap(q.data) - (q.tail - q.head)) == 0 {
		return ErrQueueFull
	}

	n := q.tail % cap(q.data)
	q.data[n] = x

	q.tail++
	return nil
}

func (q *RingQueue) Pop() (float64, error) {
	if q.tail == q.head {
		return 0, ErrQueueEmpty
	}

	n := q.head % cap(q.data)
	x := q.data[n]

	q.head++
	return x, nil
}
