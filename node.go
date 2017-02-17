package gopaxos

//All the function in class Node is thread safe!
type Node interface {
	Propose(groupIdx int, value []byte, smCtx *SMCtx) (uint64, error)
	GetNowInstanceID(groupIdx int) uint64
	GetMyNodeID() nodeId
	//Batch propose.

	//Only set options::bUserBatchPropose as true can use this batch API.
	//Warning: BatchProposal will have same llInstanceID returned but different iBatchIndex.
	//Batch values's execute order in StateMachine is certain, the return value iBatchIndex
	//means the execute order index, start from 0.
	BatchPropose(groupIdx int, value []byte, smCtx *SMCtx) (uint64, int, error)
	//PhxPaxos will batch proposal while waiting proposals count reach to BatchCount,
	//or wait time reach to BatchDelayTimeMs.
	SetBatchCount(groupIdx int, batchCount int)
	SetBatchDelayTimeMs(groupIdx int, batchDelayTimeMs int)

	//AddStateMachine adds state machine to all group.
	AddStateMachine(sm *StateMachine)

	//AddStateMachine adds state machine to a specified group
	AddStateMachineToGroup(groupIdx int, sm *StateMachine)

	//SetTimeoutMs sets the timeout interval, unit ms.
	SetTimeoutMs(timeoutMs int)

	//Checkpoint
	//Set the number you want to keep paxoslog's count.
	//We will only delete paxoslog before checkpoint instanceid.
	//If HoldCount < 300, we will set it to 300. Not suggest too small holdcount.
	SetHoldPaxosLogCount(holdCount uint64)

	//PauseCheckpointRePlayer
	PauseCheckpointRePlayer()

	//ContinueCheckpointRePlayer
	ContinueCheckpointRePlayer()

	//Paxos log cleaner working for deleting paxoslog before checkpoint instanceid.
	//Paxos log cleaner default is pausing.
	//pause paxos log cleaner.
	PausePaxosLogCleaner()

	//Continue to run paxos log cleaner.
	ContinuePaxosLogCleaner()

	//Membership
	//Show now membership.
	ShowMembership(groupIdx int, nodeInfoList NodeInfoList)

	//Add a paxos node to membership.
	AddMember(groupIdx int, node *NodeInfo)

	//Remove a paxos node from membership.
	RemoveMember(groupIdx int, node *NodeInfo)

	//Change membership by one node to another node.
	ChangeMember(groupIdx int, from, to *NodeInfo)

	//Master
	//Check who is master.
	GetMaster(groupIdx int) NodeInfo

	//Check who is master and get version.
	GetMasterWithVersion(groupIdx int) (n NodeInfo, version uint64)

	//Check is i'm master.
	IsIMMaster(groupIdx int) bool

	SetMasterLease(groupIdx int, leaseTimeMs int) error

	DropMaster(groupIdx int) error

	//Qos
	//If many threads propose same group, that some threads will be on waiting status.
	//Set max hold threads, and we will reject some propose request to avoid to many threads be holded.
	//Reject propose request will get retcode(PaxosTryCommitRet_TooManyThreadWaiting_Reject), check on def.h.
	SetMaxHoldThreads(groupIdx int, maxHoldThreads int)

	//To avoid threads be holded too long time, we use this threshold to reject some propose to control thread's wait time.
	SetProposeWaitTimeThresholdMS(groupIdx int, waitTimeThresholdMS int)

	//write disk
	SetLogSync(groupIdx int, bLogSync bool)

	//Not suggest to use this function
	//pair: value,smid.
	//Because of BatchPropose, a InstanceID maybe include multi-value.
	GetInstanceValue(groupIdx int, instanceId uint64, values []pair)

	OnReceiveMessage(msg []byte) error
}

type pair struct {
	s string
	i int
}

func RunNode(opt *Options) (Node, error) {
	if opt.IsLargeValueMode {
		getInsideOptionsInstance().setAsLargeBufferMode()
	}

	getInsideOptionsInstance().setGroupCount(opt.GroupCount)
	setBPInstance(opt.Breakpoint)

	n := newNode()

	err := n.init(opt)
	return n, err
}

// node implements the interface Node
type node struct {
	Id         nodeId
	logStorage *multiDatabase
	network    *dfNetwork
	notifyPool *notifierPool

	groupList    []*group
	masterList   []*masterMgr
	proposeBatch []*proposeBatch
}

func newNode() *node {
	return &node{
		Id:         nullNode,
		notifyPool: newNotifierPool(),
	}
}

func (n *node) init(opt *Options) error {
	return nil
}

func (n *node) initLogStorage(opt *Options) (LogStorage, error) {
	if opt.LogStorage != nil {
		lPLImp("OK, use user logstorage")
		return opt.LogStorage, nil
	}

	if opt.LogStoragePath == "" {
		lPLErr("LogStorage Path is empty")
		return nil, ErrLogStoragePathEmpty
	}

	err := n.logStorage.init(opt.LogStoragePath, opt.GroupCount)
	if err != nil {
		lPLErr("Init default logstorage fail, logpath %s err: %v",
			opt.LogStoragePath, err)
		return nil, err
	}

	lPLImp("OK, use default logstorage")

	return n.logStorage, nil
}

func (n *node) initNetwork(opt *Options) (Network, error) {
	if opt.Network != nil {
		lPLImp("OK, use user network")
		return opt.Network, nil
	}

	err := n.network.Init(opt.MyNode.GetIP(), opt.MyNode.GetPort())
	if err != nil {
		lPLErr("init default network fail, listenip %s listenport %d error: %d",
			opt.MyNode.GetIP(), opt.MyNode.GetPort(), err)
		return nil, err
	}

	lPLImp("OK, use default network")

	return n.network, nil
}

func (n *node) checkOptions(opt *Options) error {
	if opt.LogFunc != nil {
		getLoggerInstance().SetLogFunc(opt.LogFunc)
	} else {
		getLoggerInstance().InitLogger(opt.LogLevel)
	}

	if opt.LogStorage == nil && opt.LogStoragePath == "" {
		lPLErr("no logpath and logstorage is null")
		return ErrLogStoragePathEmpty
	}

	if opt.UDPMaxSize > 64*1024 {
		lPLErr("udp max size %d is too large", opt.UDPMaxSize)
		return ErrUDPMaxSizeTooLarge
	}

	if opt.GroupCount > 200 {
		lPLErr("group count %d is too large", opt.GroupCount)
		return ErrGroupCount
	}

	if opt.GroupCount <= 0 {
		lPLErr("group count %d is small than zero or equal to zero", opt.GroupCount)
		return ErrGroupCount
	}

	for _, follower := range opt.FollowerNodeInfoList {
		if follower.MyNode.GetNodeID() == follower.FollowNode.GetNodeID() {
			lPLErr("self node ip %s port %d equal to follow node",
				follower.MyNode.GetIP(), follower.MyNode.GetPort())
			return ErrSelfNodeFollowed
		}
	}

	for _, groupSM := range opt.GroupSMInfoList {
		if groupSM.GroupIdx >= opt.GroupCount {
			lPLErr("SM GroupIdx %d large than GroupCount %d",
				groupSM.GroupIdx, opt.GroupCount)
			return ErrGroupIdxOutOfRange
		}
	}

	return nil
}

func (n *node) initStateMachine(opt *Options) {
	for _, groupSM := range opt.GroupSMInfoList {
		for _, sm := range groupSM.SMList {
			n.AddStateMachineToGroup(groupSM.GroupIdx, sm)
		}
	}
}

func (n *node) runMaster(opt *Options) {
	for _, groupSM := range opt.GroupSMInfoList {
		//check if need to run master.
		if groupSM.IsUseMaster {
			if !n.groupList[groupSM.GroupIdx].getConfig().isIMFollower() {
				n.masterList[groupSM.GroupIdx].runMaster()
			} else {
				lPLImp("I'm follower, not run master damon.")
			}
		}
	}
}

func (n *node) AddStateMachineToGroup(groupIdx int, sm StateMachine) {

}

func (n *node) stop() {
	//1.step: must stop master(app) first.
	for _, m := range n.masterList {
		m.stopMaster()
	}

	//2.step: stop propose batch
	for _, p := range n.proposeBatch {
		p.stop()
	}
	//3.step: stop network.
	n.network.StopNetWork()

	//4.step: delete paxos instance. 析构
	for _, g := range n.groupList {
		g.stop()
	}
}
