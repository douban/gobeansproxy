package dstore

import (
	"errors"
	"sync"
	"time"
)

const TIMEINTERVAL = 30 // proxy 链接 后端 超时时间 为 3 秒，TIMEI

type Response struct {
	ReqTime time.Time
	count   int
	Sum     float64
}

type RingQueue struct {
	sync.Mutex
	resData []Response
	errData []Response
}

var (
	ErrQueueFull  = errors.New("queue full")
	ErrQueueEmpty = errors.New("queue empty")
)

func NewRingQueue(cap int) *RingQueue {
	return &RingQueue{
		resData: make([]Response, cap),
		errData: make([]Response, cap),
	}
}

func (q *RingQueue) Push(start time.Time, ResTime float64) error {
	second := start.Second()
	q.Lock()
	defer q.Unlock()
	// TODO 这里需要反转一下
	if q.resData[second].ReqTime.Sub(start) > TIMEINTERVAL {
		q.resData[second].Sum = ResTime
		q.resData[second].count = 1
		q.resData[second].ReqTime = start
	}
	q.resData[second].Sum += ResTime
	q.resData[second].ReqTime = start
	q.resData[second].count += 1

	return nil
}

func (q *RingQueue) PushErr(start time.Time, ResTime float64) error {
	second := start.Second()
	q.Lock()
	defer q.Unlock()
	if q.errData[second].count > 0 {
		if start.Sub(q.errData[second].ReqTime) > TIMEINTERVAL {
			q.errData[second].Sum = ResTime
			q.errData[second].count = 1
			q.errData[second].ReqTime = start
		}
	}
	q.errData[second].Sum += ResTime
	q.errData[second].ReqTime = start
	q.errData[second].count += 1

	return nil
}

func (q *RingQueue) GetResponses(num int) (responses []Response) {
	now := time.Now()
	second := now.Second()
	offset := second - num
	if offset > 0 {
		return q.resData[offset:second]
	} else {
		return append(q.resData[len(q.resData)+offset:], q.resData[0:second]...)
	}
}

func (q *RingQueue) GetErrors(num int) (responses []Response) {
	now := time.Now()
	second := now.Second()
	offset := second - num
	if offset < 0 {
		return append(q.errData[len(q.errData)+offset:], q.errData[0:second]...)
	} else {
		return q.errData[offset:second]
	}
}
