package paxos

type group struct {
	comm     *communicate
	conf     *config
	instance *instance
	initRet  error
	ch       chan struct{}
}

func newGroup(ls LogStorage, network Network, masterSM insideSM, groupIdx int, opt *Options) *group {
	ret := &group{}

	ret.conf = newConfig(ls, opt.Sync, opt.SyncInterval, opt.UseMembership, &opt.MyNode,
		opt.NodeInfoList, opt.FollowerNodeInfoList, groupIdx, opt.GroupCount, opt.MembershipChangeCallback)

	ret.comm = newCommunicate(ret.conf, opt.MyNode.GetNodeID(), opt.UDPMaxSize, network)

	ret.instance = newInstance(ret.conf, ls, ret.comm, opt)

	ret.initRet = -1

	return ret
}

func (g *group) startInit() {
	g.ch = make(chan struct{}, 1)
	go g.init()
}

func (g *group) init() {
	g.initRet = g.conf.init()
	if g.initRet != nil {
		return
	}

	g.addStateMachine(g.conf.getSystemVSM())
	g.addStateMachine(g.conf.getMasterSM())

	g.initRet = g.instance.init()
}

func (g *group) getInitRet() error {
	<-g.ch
	return g.initRet
}

func (g *group) start() {
	g.instance.start()
}

func (g *group) getConfig() *config {
	return g.conf
}

func (g *group) getInstance() *instance {
	return g.instance
}

func (g *group) getCommitter() *committer {
	return g.instance.getCommitter()
}

func (g *group) getCheckpointCleaner() *cleaner {
	return g.instance.getCheckpointCleaner()
}

func (g *group) getCheckpointRePlayer() *rePlayer {
	return g.instance.getCheckpointRePlayer()
}

func (g *group) addStateMachine(sm StateMachine) {
	g.instance.addStateMachine(sm)
}

func (g *group) stop() {

}
