package dstore

import (
	"errors"
	"sync"
	"time"
)

const (
	TIMEINTERVAL = 30 * 1000 * 1000 * 1000 // proxy 链接 后端 超时时间 为 3 秒，清空 30s 之前的数据，30 * 1000 * 1000 * 1000
	QUEUECAP     = 60
)

const (
	latencyData = iota
	errorData
)

type Response struct {
	ReqTime time.Time
	Count   int
	Sum     float64
}

type RingQueue struct {
	resData *[QUEUECAP]Response
	errData *[QUEUECAP]Response
	sync.RWMutex
}

var (
	ErrQueueFull  = errors.New("queue full")
	ErrQueueEmpty = errors.New("queue empty")
)

func NewRingQueue() *RingQueue {
	return &RingQueue{
		resData: &[QUEUECAP]Response{},
		errData: &[QUEUECAP]Response{},
	}
}

func (q *RingQueue) Push(start time.Time, ResTime float64, dataType int) error {
	second := start.Second()
	var data *[QUEUECAP]Response
	switch dataType {
	case latencyData:
		data = q.resData
	case errorData:
		data = q.errData
	}

	// TODO errData/ resData 锁分开:
	q.Lock()
	defer q.Unlock()
	if start.Sub(data[second].ReqTime) > TIMEINTERVAL {
		data[second].Sum = ResTime
		data[second].Count = 1
		data[second].ReqTime = start
	}
	data[second].Sum += ResTime
	data[second].ReqTime = start
	data[second].Count++

	return nil
}

func (q *RingQueue) Get(num, dataType int) (responses []Response) {
	now := time.Now()
	second := now.Second()
	offset := second - num

	var data *[QUEUECAP]Response
	switch dataType {
	case latencyData:
		data = q.resData
	case errorData:
		data = q.errData
	}
	q.RLock()
	defer q.RUnlock()
	if offset > 0 {
		return data[offset:second]
	} else {
		return append(data[len(q.resData)+offset:], data[0:second]...)
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
