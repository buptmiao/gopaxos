package gopaxos

type committer struct {
	conf        *config
	ctx         *commitCtx
	loop        *ioLoop
	smFac       *smFac
	lock        *waitLock
	timeoutMs   int
	lastLogTime uint64
}

func newCommitter(conf *config, ctx *commitCtx, loop *ioLoop, smFac *smFac) *committer {
	ret := &committer{}
	ret.conf = conf
	ret.ctx = ctx
	ret.loop = loop
	ret.smFac = smFac
	ret.timeoutMs = -1
	ret.lastLogTime = getSteadyClockMS()

	return ret
}

func (c *committer) newValue(value []byte) int {
	_, ret := c.newValueGetID(value, nil)
	return ret
}

func (c *committer) newValueGetID(value []byte, smCtx *SMCtx) (uint64, int) {
	getBPInstance().NewValue()

	ret := paxostrycommitret_ok
	var instanceID uint64
	for retryCount := 3; retryCount > 0; retryCount-- {
		ts := timeStat(0)
		ts.point()

		instanceID, ret = c.newValueGetIDNoRetry(value, smCtx)
		if ret != paxostrycommitret_conflict {
			if ret == 0 {
				getBPInstance().NewValueCommitOK(ts.point())
			} else {
				getBPInstance().NewValueCommitFail()
			}

			break
		}

		getBPInstance().NewValueConflict()

		if smCtx != nil && smCtx.SMID == master_v_smid {
			//master sm not retry
			break
		}
	}

	return instanceID, ret
}

func (c *committer) newValueGetIDNoRetry(value []byte, smCtx *SMCtx) (uint64, int) {
	c.logStatus()

	lockUseTimeMs, hasLock := c.lock.lock(c.timeoutMs)
	if !hasLock {
		if lockUseTimeMs > 0 {
			getBPInstance().NewValueGetLockTimeout()
			lPLGErr(c.conf.groupIdx, "Try get lock, but timeout, lockusetime %dms", lockUseTimeMs)
			return 0, paxostrycommitret_timeout
		} else {
			getBPInstance().NewValueGetLockReject()
			lPLGErr(c.conf.groupIdx, "Try get lock, but too many thread waiting, reject")
			return 0, paxostrycommitret_toomanythreadwaiting_reject
		}
	}

	defer c.lock.unlock()
	leftTimeoutMs := -1
	if c.timeoutMs > 0 {
		leftTimeoutMs = 0
		if c.timeoutMs > lockUseTimeMs {
			leftTimeoutMs = c.timeoutMs - lockUseTimeMs
		}

		if leftTimeoutMs < 200 {
			lPLGErr(c.conf.groupIdx, "Get lock ok, but lockusetime %dms too long, lefttimeout %dms", lockUseTimeMs, leftTimeoutMs)
			getBPInstance().NewValueGetLockTimeout()

			return 0, paxostrycommitret_timeout
		}
	}

	lPLGImp(c.conf.groupIdx, "GetLock ok, use time %dms", lockUseTimeMs)

	getBPInstance().NewValueGetLockOK(lockUseTimeMs)

	//pack smid to value
	var smID int64
	if smCtx != nil {
		smID = smCtx.SMID
	}

	packSMIDValue := make([]byte, len(value))
	copy(packSMIDValue, value)

	packSMIDValue = c.smFac.packPaxosValue(packSMIDValue, smID)
	c.ctx.newCommit(packSMIDValue, smCtx, leftTimeoutMs)
	c.loop.addNotify()

	return c.ctx.getResult()
}

func (c *committer) setTimeoutMs(timeoutMs int) {
	c.timeoutMs = timeoutMs
}

func (c *committer) setMaxHoldThreads(maxHoldThreads int) {
	c.lock.setMaxWaitLogCount(maxHoldThreads)
}

func (c *committer) setProposeWaitTimeThresholdMs(waitTimeThresholdMs int) {
	c.lock.setLockWaitTimeThreshold(waitTimeThresholdMs)
}

func (c *committer) logStatus() {
	now := getSteadyClockMS()
	if now > c.lastLogTime && now-c.lastLogTime > 1000 {
		c.lastLogTime = now
		lPLGStatus(c.conf.groupIdx, "wait threads %d avg thread wait ms %d reject rate %d",
			c.lock.getNowHoldThreadCount(), c.lock.getNowAvgThreadWaitTime(),
			c.lock.getNowRejectRate())
	}
}
