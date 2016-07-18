package dstore

import (
	"errors"
	"sync"
	"time"
)

const TIMEINTERVAL = 30 // proxy 链接 后端 超时时间 为 3 秒，TIMEI

const QUEUECAP = 60

type Response struct {
	ReqTime time.Time
	Count   int
	Sum     float64
}

type RingQueue struct {
	resData [QUEUECAP]Response
	errData [QUEUECAP]Response
	sync.RWMutex
}

var (
	ErrQueueFull  = errors.New("queue full")
	ErrQueueEmpty = errors.New("queue empty")
)

func NewRingQueue() *RingQueue {
	return &RingQueue{
		resData: [QUEUECAP]Response{},
		errData: [QUEUECAP]Response{},
	}
}

func (q *RingQueue) Push(start time.Time, ResTime float64) error {
	second := start.Second()
	q.Lock()
	defer q.Unlock()
	if start.Sub(q.resData[second].ReqTime) > TIMEINTERVAL {
		q.resData[second].Sum = ResTime
		q.resData[second].Count = 1
		q.resData[second].ReqTime = start
	}
	q.resData[second].Sum += ResTime
	q.resData[second].ReqTime = start
	q.resData[second].Count += 1

	return nil
}

func (q *RingQueue) PushErr(start time.Time, ResTime float64) error {
	second := start.Second()
	q.Lock()
	defer q.Unlock()
	if q.errData[second].Count > 0 {
		if start.Sub(q.errData[second].ReqTime) > TIMEINTERVAL {
			q.errData[second].Sum = ResTime
			q.errData[second].Count = 1
			q.errData[second].ReqTime = start
		}
	}
	q.errData[second].Sum += ResTime
	q.errData[second].ReqTime = start
	q.errData[second].Count += 1

	return nil
}

// get responses in last num seconds
func (q *RingQueue) GetResponses(num int) (responses []Response) {
	q.RLock()
	defer q.RUnlock()
	now := time.Now()
	second := now.Second()
	offset := second - num
	if offset > 0 {
		return q.resData[offset:second]
	} else {
		return append(q.resData[len(q.resData)+offset:], q.resData[0:second]...)
	}
}

// get errors in last num seconds
func (q *RingQueue) GetErrors(num int) (responses []Response) {
	q.RLock()
	defer q.RUnlock()
	now := time.Now()
	second := now.Second()
	offset := second - num
	if offset < 0 {
		return append(q.errData[len(q.errData)+offset:], q.errData[0:second]...)
	} else {
		return q.errData[offset:second]
	}
}

func (q *RingQueue) clear() {
	q.Lock()
	q.Unlock()
	for i := 0; i < QUEUECAP; i++ {
		q.errData[i] = Response{}
		q.resData[i] = Response{}
	}
}
