package gopaxos

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/buptmiao/gopaxos/paxospb"
)

type notifier struct {
	pipe chan int
}

func newNotifier() *notifier {
	return &notifier{
		pipe: make(chan int, 10),
	}
}

func (n *notifier) SendNotify(v int) {
	n.pipe <- v
}

func (n *notifier) WaitNotify() int {
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
	defer n.RUnlock()
	return n.pool[id]
}

func (n *notifierPool) addNotifier(id uint64, v *notifier) {
	n.Lock()
	defer n.Unlock()
	n.pool[id] = v
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
	now := time.Now().UnixNano() / time.Millisecond
	return uint64(now)
}

type timeStat uint64

func (t timeStat) point() int {
	now := getSteadyClockMS()
	var passTime int
	if now > t {
		passTime = now - t
	}
	t = now
	return passTime
}

func makeOpValue(id nodeId, version uint64, timeout int, op masterOperatorType) ([]byte, error) {
	oper := &paxospb.MasterOperator{}

	oper.Nodeid = id
	oper.Version = version
	oper.Timeout = timeout
	oper.Operator = op
	oper.Sid = rand.Uint32()

	return oper.Marshal()
}
