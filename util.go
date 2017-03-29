package gopaxos

import (
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"container/heap"
	"github.com/buptmiao/gopaxos/paxospb"
	"math"
)

type notifier struct {
	pipe chan error
}

func newNotifier() *notifier {
	return &notifier{
		pipe: make(chan error, 10),
	}
}

func (n *notifier) SendNotify(v error) {
	n.pipe <- v
}

func (n *notifier) WaitNotify() error {
	return <-n.pipe
}

type notifierPool struct {
	sync.RWMutex
	pool map[uint64]*notifier
}

func newNotifierPool() *notifierPool {
	return &notifierPool{
		pool: make(map[uint64]*notifier),
	}
}

func (n *notifierPool) getNotifier(id uint64) *notifier {
	n.RLock()
	ret := n.pool[id]
	n.RUnlock()
	if ret != nil {
		return ret
	}

	ret = newNotifier()
	n.Lock()
	n.pool[id] = ret
	n.Unlock()

	return ret
}

///////////////////////////////////////////////////////////////////////////////
// file util
//
///////////////////////////////////////////////////////////////////////////////
func isDir(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fi.IsDir(), nil
}

func deleteDir(path string) error {
	return os.RemoveAll(path)
}

func iterDir(path string) ([]string, error) {
	var ret []string
	walkFn := func(p string, f os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if !f.IsDir() {
			ret = append(ret, p)
		}
		return nil
	}
	err := filepath.Walk(path, walkFn)
	return ret, err
}

///////////////////////////////////////////////////////////////////////////////
// time stat
//
///////////////////////////////////////////////////////////////////////////////
func getSteadyClockMS() uint64 {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	return uint64(now)
}

type timeStat uint64

func (t timeStat) point() int {
	now := getSteadyClockMS()
	var passTime int
	if now > uint64(t) {
		passTime = int(now - uint64(t))
	}
	t = timeStat(now)
	return passTime
}

///////////////////////////////////////////////////////////////////////////////
// serial lock
//
///////////////////////////////////////////////////////////////////////////////
type serialLock struct {
	mu sync.Mutex
	c  chan struct{}
}

func newSerialLock() *serialLock {
	return &serialLock{
		c: make(chan struct{}),
	}
}

func (s *serialLock) lock() {
	s.mu.Lock()
}

func (s *serialLock) unlock() {
	s.mu.Unlock()
}

func (s *serialLock) wait() {
	s.mu.Unlock()
	<-s.c
	s.mu.Lock()
}

func (s *serialLock) interrupt() {
	close(s.c)
	s.c = make(chan struct{})
}

func (s *serialLock) broadcast() {
	s.mu.Lock()
	close(s.c)
	s.c = make(chan struct{})
	s.mu.Unlock()
}

// timeout return false.
func (s *serialLock) waitTime(timeout time.Duration) bool {
	s.mu.Unlock()
	defer s.mu.Lock()
	select {
	case <-s.c:
		return true
	case <-time.After(timeout):
		return false
	}
}

///////////////////////////////////////////////////////////////////////////////
// wait lock
//
///////////////////////////////////////////////////////////////////////////////

const wait_Lock_UserTime_Avg_Interval = 250

type waitLock struct {
	slock                   *serialLock
	isLockUsing             bool
	waitLockCount           int
	maxWaitLockCount        int
	lockUseTimeSum          int
	avgLockUseTime          int
	lockUseTimeCount        int
	rejectRate              int
	lockWaitTimeThresholdMs int
}

func newWaitLock() *waitLock {
	return &waitLock{
		isLockUsing:             false,
		waitLockCount:           0,
		maxWaitLockCount:        -1,
		lockUseTimeSum:          0,
		avgLockUseTime:          0,
		lockUseTimeCount:        0,
		rejectRate:              0,
		lockWaitTimeThresholdMs: -1,
		slock: newSerialLock(),
	}
}

func (w *waitLock) canLock() bool {
	if w.maxWaitLockCount != -1 && w.waitLockCount >= w.maxWaitLockCount {
		// too much lock waiting
		return false
	}

	if w.lockWaitTimeThresholdMs == -1 {
		return true
	}

	return int(rand.Uint32()%100) >= w.rejectRate
}

func (w *waitLock) refreshRejectRate(useTimeMs int) {
	if w.lockWaitTimeThresholdMs == -1 {
		return
	}

	w.lockUseTimeSum += useTimeMs
	w.lockUseTimeCount++
	if w.lockUseTimeCount >= wait_Lock_UserTime_Avg_Interval {
		w.avgLockUseTime = w.lockUseTimeSum / w.lockUseTimeCount
		w.lockUseTimeSum = 0
		w.lockUseTimeCount = 0

		if w.avgLockUseTime > w.lockWaitTimeThresholdMs {
			w.rejectRate = int(math.Min(98, float64(w.rejectRate+3)))
		} else {
			w.rejectRate = int(math.Max(0, float64(w.rejectRate-3)))
		}
	}
}

func (w *waitLock) setMaxWaitLogCount(maxWaitLockCount int) {
	w.maxWaitLockCount = maxWaitLockCount
}

func (w *waitLock) setLockWaitTimeThreshold(lockWaitTimeThresholdMs int) {
	w.lockWaitTimeThresholdMs = lockWaitTimeThresholdMs
}

func (w *waitLock) lock(timeoutMs int) (int, bool) {
	beginTime := getSteadyClockMS()

	w.slock.lock()
	defer w.slock.unlock()

	if !w.canLock() {
		return 0, false
	}

	w.waitLockCount++
	getLock := true

	for w.isLockUsing {
		if timeoutMs == -1 {
			w.slock.waitTime(time.Millisecond * 1000)
			continue
		}

		if !w.slock.waitTime(time.Millisecond * time.Duration(timeoutMs)) {
			//lock timeout
			getLock = false
			break
		}
	}

	w.waitLockCount--

	endTime := getSteadyClockMS()
	var useTimeMs int
	if endTime > beginTime {
		useTimeMs = int(endTime - beginTime)
	}

	w.refreshRejectRate(useTimeMs)

	if getLock {
		w.isLockUsing = true
	}

	return useTimeMs, getLock
}

func (w *waitLock) unlock() {
	w.slock.lock()
	defer w.slock.unlock()

	w.isLockUsing = false
	w.slock.interrupt()
}

func (w *waitLock) getNowHoldThreadCount() int {
	return w.waitLockCount
}

func (w *waitLock) getNowAvgThreadWaitTime() int {
	return w.avgLockUseTime
}

func (w *waitLock) getNowRejectRate() int {
	return w.rejectRate
}

///////////////////////////////////////////////////////////////////////////////
// timer
//
///////////////////////////////////////////////////////////////////////////////
type timerObj struct {
	timerID uint32
	absTime uint64
	typ     timerType
}

func newTimerObj(timerID uint32, absTime uint64, typ timerType) *timerObj {
	return &timerObj{
		timerID: timerID,
		absTime: absTime,
		typ:     typ,
	}
}

type timer struct {
	nowTimerID uint32
	timerHeap  []*timerObj
}

func newTimer() *timer {
	return &timer{
		nowTimerID: 1,
	}
}

func (t *timer) Len() int {
	return len(t.timerHeap)
}

func (t *timer) Less(i, j int) bool {
	if t.timerHeap[i].absTime == t.timerHeap[j].absTime {
		return t.timerHeap[i].timerID < t.timerHeap[j].timerID
	}

	return t.timerHeap[i].absTime < t.timerHeap[j].absTime
}

func (t *timer) Swap(i, j int) {
	t.timerHeap[i], t.timerHeap[j] = t.timerHeap[j], t.timerHeap[i]
}

func (t *timer) Push(x interface{}) {
	item := x.(*timerObj)
	t.timerHeap = append(t.timerHeap, item)
}

func (t *timer) Pop() interface{} {
	n := len(t.timerHeap)
	item := t.timerHeap[n-1]
	t.timerHeap = t.timerHeap[0 : n-1]
	return item
}

func (t *timer) addTimer(absTime uint64) uint32 {
	return t.addTimerWithType(absTime, 0)
}

func (t *timer) addTimerWithType(absTime uint64, typ timerType) uint32 {
	timerID := t.nowTimerID
	t.nowTimerID++

	heap.Push(t, newTimerObj(timerID, absTime, typ))
	return timerID
}

func (t *timer) popTimeout() (uint32, timerType, bool) {
	if len(t.timerHeap) == 0 {
		return 0, 0, false
	}

	obj := t.timerHeap[0]
	now := getSteadyClockMS()
	if obj.absTime > now {
		return 0, 0, false
	}
	heap.Pop(t)
	return obj.timerID, obj.typ, true
}

func (t *timer) getNextTimeout() int {
	if len(t.timerHeap) == 0 {
		return -1
	}

	var nextTimeout int
	obj := t.timerHeap[0]
	now := getSteadyClockMS()
	if obj.absTime > now {
		nextTimeout = int(obj.absTime - now)
	}

	return nextTimeout
}

///////////////////////////////////////////////////////////////////////////////
// others
//
///////////////////////////////////////////////////////////////////////////////
func makeOpValue(id uint64, version uint64, timeout int, op masterOperatorType) ([]byte, error) {
	oper := &paxospb.MasterOperator{}

	oper.Nodeid = id
	oper.Version = version
	oper.Timeout = int32(timeout)
	oper.Operator = uint32(op)
	oper.Sid = rand.Uint32()

	return oper.Marshal()
}

func getGid(id uint64) uint64 {
	return (id ^ uint64(rand.Uint32())) + uint64(rand.Uint32())
}

func crc(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crc32.IEEETable, data)
}

var count uint64

func getUniqueID() uint64 {
	high := getSteadyClockMS() << 23
	low := count

	count++
	if count == 1<<23 {
		count = 0
	}

	return high + low
}
