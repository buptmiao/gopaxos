package gopaxos

type instance struct {
	conf          *config
	comm          MsgTransport
	smFac         *smFac
	loop          *ioLoop
	acceptor      *acceptor
	learner       *learner
	proposer      *proposer
	paxosLog      *paxosLog

	lastChecksum  uint32
	commitCtx     *commitCtx
	commitTimerID uint32
	committer     *committer

	checkpointMgr *checkpointMgr

	timeStat      timeStat
	opt           *Options
}

func newInstance(conf *config, ls LogStorage, tran MsgTransport, opt *Options) *instance {
	ret := &instance{}

	ret.smFac = newSMFac(conf.getMyGroupIdx())
	ret.loop = newIOLoop(conf, ret)
	ret.acceptor = newAcceptor(conf, tran, ret, ls)
	ret.checkpointMgr = newCheckpointMgr(conf, ret.smFac, ls, opt.IsUseCheckpointRePlayer)
	ret.learner = newLearner(conf, tran, ret, ret.acceptor, ls, ret.loop, ret.checkpointMgr, ret.smFac)
	ret.proposer = newProposer(conf, tran, ret, ret.learner, ret.loop)
	ret.commitCtx = newCommitCtx(conf)
	ret.committer = newCommitter(conf, ret.commitCtx, ret.loop, ret.smFac)
	ret.opt = opt

	ret.conf = conf
	ret.comm = tran
	ret.commitTimerID = 0
	ret.lastChecksum = 0

	return ret
}

func (i *instance) init() error {

}

func (i *instance) start() {

}

func (i *instance) getCommitter() *committer {

}

func (i *instance) getCheckpointCleaner() *cleaner {

}

func (i *instance) getCheckpointRePlayer() *rePlayer {

}

func (i *instance) addStateMachine(sm StateMachine) {

}

func (i *instance) stop() {
	i.loop.stop()
	i.checkpointMgr.stop()
	i.learner.stop()
}
