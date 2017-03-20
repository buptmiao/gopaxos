package gopaxos

type checkpointMgr struct {
	conf                     *config
	logStorage               LogStorage
	smFac                    *smFac
	rePlayer                 *rePlayer
	cleaner                  *cleaner
	minChosenInstanceID      uint64
	maxChosenInstanceID      uint64
	inAskForCPMode           bool
	needAckSet               map[uint64]struct{}
	lastAskForCheckpointTime uint64
	useCheckpointRePlayer    bool
}

func newCheckpointMgr(conf *config, smFac *smFac, ls LogStorage, useCPRePlayer bool) *checkpointMgr {
	ret := &checkpointMgr{}
	ret.conf = conf
	ret.logStorage = ls
	ret.smFac = smFac
	ret.rePlayer = newRePlayer(conf, smFac, ls, ret)
	ret.cleaner = newCleaner(conf, smFac, ls, ret)
	ret.useCheckpointRePlayer = useCPRePlayer

	return ret
}

func (c *checkpointMgr) init() error {
	var err error
	if c.minChosenInstanceID, err = c.logStorage.GetMinChosenInstanceID(c.conf.groupIdx); err != nil {
		return err
	}

	if err = c.cleaner.fixMinChosenInstanceID(c.minChosenInstanceID); err != nil {
		return err
	}

	return nil
}

func (c *checkpointMgr) start() {
	if c.useCheckpointRePlayer {
		c.rePlayer.start()
	}

	c.cleaner.start()
}

func (c *checkpointMgr) stop() {
	if c.useCheckpointRePlayer {
		c.rePlayer.stop()
	}

	c.cleaner.stop()
}

func (c *checkpointMgr) getRePlayer() *rePlayer {
	return c.rePlayer
}

func (c *checkpointMgr) getCleaner() *cleaner {
	return c.cleaner
}

func (c *checkpointMgr) prepareForAskForCheckpoint(sendNodeID uint64) error {
	if _, ok := c.needAckSet[sendNodeID]; !ok {
		c.needAckSet[sendNodeID] = struct{}{}
	}

	if c.lastAskForCheckpointTime == 0 {
		c.lastAskForCheckpointTime = getSteadyClockMS()
	}

	now := getSteadyClockMS()
	if now > c.lastAskForCheckpointTime+60000 {
		lPLGImp(c.conf.groupIdx, "no majority reply, just ask for checkpoint")
	} else {
		if len(c.needAckSet) < c.conf.getMajorityCount() {
			lPLGImp(c.conf.groupIdx, "Need more other tell us need to askforcheckpoint")
			return errCheckpointMissMajority
		}
	}

	c.lastAskForCheckpointTime = 0
	c.inAskForCPMode = true

	return nil
}

func (c *checkpointMgr) inAskForCheckpointMode() bool {
	return c.inAskForCPMode
}

func (c *checkpointMgr) exitCheckpointMode() {
	c.inAskForCPMode = false
}

func (c *checkpointMgr) getCheckpointInstanceID() uint64 {
	return c.smFac.getCheckpointInstanceID(c.conf.groupIdx)
}

func (c *checkpointMgr) getMinChosenInstanceID() uint64 {
	return c.minChosenInstanceID
}

func (c *checkpointMgr) setMinChosenInstanceID(minChosenInstanceID uint64) error {
	wo := writeOptions(true)
	if err := c.logStorage.SetMinChosenInstanceID(wo, c.conf.groupIdx, minChosenInstanceID); err != nil {
		return err
	}

	c.minChosenInstanceID = minChosenInstanceID

	return nil
}

func (c *checkpointMgr) setMinChosenInstanceIDCache(minChosenInstanceID uint64) {
	c.minChosenInstanceID = minChosenInstanceID
}

func (c *checkpointMgr) setMaxChosenInstanceID(maxChosenInstanceID uint64) {
	c.maxChosenInstanceID = maxChosenInstanceID
}

func (c *checkpointMgr) getMaxChosenInstanceID() uint64 {
	return c.maxChosenInstanceID
}
