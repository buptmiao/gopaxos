package gopaxos

import (
	"bytes"
	"time"
)

type commitCtx struct {
	conf        *config
	instanceID  uint64
	commitRet   int
	isCommitEnd bool
	timeoutMs   int
	value       []byte
	smCtx       *SMCtx
	slock       *serialLock
}

func newCommitCtx(conf *config) *commitCtx {
	ret := &commitCtx{}
	ret.newCommit(nil, nil, 0)
	return ret
}

func (c *commitCtx) newCommit(value []byte, ctx *SMCtx, timeoutMs int) {
	c.slock.lock()
	defer c.slock.unlock()

	c.instanceID = uint64(-1)
	c.commitRet = -1
	c.isCommitEnd = false
	c.timeoutMs = timeoutMs

	c.value = value
	c.smCtx = ctx

	if value != nil {
		lPLGHead(c.conf.groupIdx, "OK, valuesize %d", len(c.value))
	}
}

func (c *commitCtx) isNewCommit() bool {
	return c.instanceID == uint64(-1) && c.value != nil
}

func (c *commitCtx) getCommitValue() []byte {
	return c.value
}

func (c *commitCtx) startCommit(instanceID uint64) {
	c.slock.lock()
	defer c.slock.unlock()

	c.instanceID = instanceID
}

func (c *commitCtx) isMyCommit(instanceID uint64, learnValue []byte) (*SMCtx, bool) {
	c.slock.lock()
	defer c.slock.unlock()

	isMyCommit := false

	if !c.isCommitEnd && c.instanceID == instanceID {
		isMyCommit = bytes.Equal(learnValue, c.value)
	}

	if isMyCommit {
		return c.smCtx, true
	}

	return nil, isMyCommit
}

func (c *commitCtx) setResultOnlyRet(commitRet int) {
	c.setResult(commitRet, uint64(-1), nil)
}

func (c *commitCtx) setResult(commitRet int, instanceID uint64, learnValue []byte) {
	c.slock.lock()
	defer c.slock.unlock()

	if c.isCommitEnd || c.instanceID != instanceID {
		return
	}

	c.commitRet = commitRet
	if c.commitRet == 0 {
		if !bytes.Equal(learnValue, c.value) {
			c.commitRet = paxostrycommitret_conflict
		}
	}

	c.isCommitEnd = true
	c.value = nil
	c.slock.interrupt()
}

func (c *commitCtx) getResult() (uint64, int) {
	c.slock.lock()
	defer c.slock.unlock()

	var succInstanceID uint64
	for !c.isCommitEnd {
		c.slock.waitTime(time.Millisecond * 1000)
	}

	if c.commitRet == 0 {
		succInstanceID = c.instanceID
		lPLGImp(c.conf.groupIdx, "commit success, instanceid %lu", succInstanceID)
	} else {
		lPLGErr(c.conf.groupIdx, "commit fail, error: %d", c.commitRet)
	}

	return succInstanceID, c.commitRet
}

func (c *commitCtx) setCommitValue(value []byte) {
	c.value = value
}

func (c *commitCtx) getTimeoutMs() int {
	return c.timeoutMs
}
