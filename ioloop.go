package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

const (
	retryQueueMaxLen = 300
)

type ioLoop struct {
}

func newIOLoop(conf *config, i *instance) *ioLoop {

}

func (i *ioLoop) start() {

}

func (i *ioLoop) addTimer(timeout int, typ int) (uint32, bool) {

}

func (i *ioLoop) removeTimer(timerID uint32) {

}

func (i *ioLoop) addMessage(msg []byte) error {

}

func (i *ioLoop) addRetryPaxosMsg(paxosMsg *paxospb.PaxosMsg) error {

}

func (i *ioLoop) clearRetryQueue()

func (i *ioLoop) stop() {

}
