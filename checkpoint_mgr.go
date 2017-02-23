package gopaxos

type checkpointMgr struct {
}

func newCheckpointMgr(conf *config, smFac *smFac, ls LogStorage, useCPRePlayer bool) *checkpointMgr {

}

func (c *checkpointMgr) init() error {

}

func (c *checkpointMgr) start() {

}

func (c *checkpointMgr) getCheckpointInstanceID() uint64 {

}

func (c *checkpointMgr) getMinChosenInstanceID() uint64 {

}

func (c *checkpointMgr) setMinChosenInstanceID(instanceID uint64) error {

}

func (c *checkpointMgr) setMaxChosenInstanceID(instanceID uint64) {

}

func (c *checkpointMgr) getCleaner() *cleaner {

}

func (c *checkpointMgr) getRePlayer() *rePlayer {

}

func (c *checkpointMgr) inAskForCheckpointMode() bool {

}

func (c *checkpointMgr) stop() {

}
