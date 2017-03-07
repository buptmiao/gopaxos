package gopaxos

import (
	"container/list"
	"github.com/buptmiao/gopaxos/paxospb"
	"sync/atomic"
	"time"
)

const (
	retryQueueMaxLen = 300
)

type ioLoop struct {
	isEnd        bool
	isStart      bool
	timer        *timer
	timerIDMap   map[uint32]bool
	messageQueue chan []byte
	retryQueue   *list.List
	queueMemSize int64
	conf         *config
	instance     *instance
	quit         chan struct{}
}

func newIOLoop(conf *config, i *instance) *ioLoop {
	return &ioLoop{
		conf:         conf,
		instance:     i,
		messageQueue: make(chan []byte, getInsideOptionsInstance().getMaxIOLoopQueueLen()),
	}
}

func (i *ioLoop) start() {
	i.quit = make(chan struct{}, 1)
	go i.run()
}

func (i *ioLoop) run() {
	i.isEnd = false
	i.isStart = true

	for {
		getBPInstance().OneLoop()

		nextTimeout := i.dealWithTimeout()
		if nextTimeout <= 0 {
			nextTimeout = 1000
		}

		select {
		case msg := <-i.messageQueue:
			if len(msg) != 0 {
				atomic.AddInt64(&i.queueMemSize, -len(msg))
				i.instance.onReceive(msg)

				getBPInstance().OutQueueMsg()
			}

		case <-time.After(time.Millisecond * nextTimeout):
			break
		}

		i.dealWithRetry()
		//must put on here
		//because addTimer on this function
		i.instance.checkNewValue()

		if i.isEnd {
			lPLGHead(i.conf.groupIdx, "IOLoop [End]")
			close(i.quit)
			break
		}
	}
}

func (i *ioLoop) addTimer(timeout int, typ timerType) (uint32, bool) {
	if timeout == -1 {
		return 0, true
	}

	absTime := getSteadyClockMS() + timeout
	timerID := i.timer.addTimerWithType(absTime, typ)
	i.timerIDMap[timerID] = true
	return timerID, true
}

func (i *ioLoop) removeTimer(timerID uint32) uint32 {
	delete(i.timerIDMap, timerID)
	return 0
}

func (i *ioLoop) addNotify() {
	i.messageQueue <- nil
}

func (i *ioLoop) addMessage(msg []byte) error {
	getBPInstance().EnqueueMsg()

	if len(i.messageQueue) > getInsideOptionsInstance().getMaxIOLoopQueueLen() {
		getBPInstance().EnqueueMsgRejectByFullQueue()

		lPLGErr(i.conf.groupIdx, "Queue full, skip msg")
		return errMsgQueueFull
	}

	if atomic.LoadInt64(&i.queueMemSize) > max_Queue_Mem_Size {
		lPLGErr(i.conf.groupIdx, "queue memsize %d too large, can't enqueue", i.queueMemSize)
		return errQueueMemExceed
	}

	i.messageQueue <- msg
	atomic.AddInt64(&i.queueMemSize, len(msg))

	return nil
}

func (i *ioLoop) addRetryPaxosMsg(paxosMsg *paxospb.PaxosMsg) error {
	getBPInstance().EnqueueRetryMsg()

	if i.retryQueue.Len() > retryQueueMaxLen {
		getBPInstance().EnqueueRetryMsgRejectByFullQueue()
		i.retryQueue.Remove(i.retryQueue.Front())
	}

	i.retryQueue.PushBack(paxosMsg)
	return nil
}

func (i *ioLoop) clearRetryQueue() {
	for i.retryQueue.Len() > 0 {
		i.retryQueue.Remove(i.retryQueue.Front())
	}
}

func (i *ioLoop) dealWithRetry() {
	if i.retryQueue.Len() == 0 {
		return
	}

	haveRetryOne := false

	for i.retryQueue.Len() > 0 {
		element := i.retryQueue.Front()
		paxosMsg := element.Value.(*paxospb.PaxosMsg)
		if paxosMsg.GetInstanceID() > i.instance.getNowInstanceID()+1 {
			break
		} else if paxosMsg.GetInstanceID() == i.instance.getNowInstanceID()+1 {
			//only after retry i == now_i, than we can retry i + 1.
			if haveRetryOne {
				getBPInstance().DealWithRetryMsg()
				lPLGDebug(i.conf.groupIdx, "retry msg (i+1). instanceid %d", paxosMsg.GetInstanceID())
				i.instance.onReceivePaxosMsg(paxosMsg, true)
			} else {
				break
			}
		} else if paxosMsg.GetInstanceID() == i.instance.getNowInstanceID() {
			getBPInstance().DealWithRetryMsg()
			lPLGDebug(i.conf.groupIdx, "retry msg. instanceid %d", paxosMsg.GetInstanceID())
			i.instance.onReceivePaxosMsg(paxosMsg, false)
			haveRetryOne = true
		}

		i.retryQueue.Remove(element)
	}
}

func (i *ioLoop) dealWithTimeoutOne(timerID uint32, typ timerType) {
	if _, ok := i.timerIDMap[timerID]; !ok {
		return
	}
	delete(i.timerIDMap, timerID)
	i.instance.onTimeout(timerID, typ)
}

// deal with events those are timeout, and return the next timeout interval.
func (i *ioLoop) dealWithTimeout() int {
	hasTimeout := true

	for hasTimeout {
		timerID, typ, hasTimeout := i.timer.popTimeout()

		if hasTimeout {
			i.dealWithTimeoutOne(timerID, typ)
			nextTimeout := i.timer.getNextTimeout()
			if nextTimeout != 0 {
				return nextTimeout
			}
		}
	}

	return 0
}

func (i *ioLoop) stop() {
	i.isEnd = true
	if i.isStart && i.quit != nil {
		<-i.quit
		i.quit = nil
	}
}
