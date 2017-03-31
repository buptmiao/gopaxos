package gopaxos

import (
	"encoding/binary"
	"github.com/buptmiao/gopaxos/paxospb"
	"math"
)

//All the function in class Node is thread safe!
type Node interface {
	Propose(groupIdx int, value []byte, smCtx *SMCtx) (uint64, error)
	GetNowInstanceID(groupIdx int) uint64
	GetMyNodeID() uint64
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
	AddStateMachine(sm StateMachine)

	//AddStateMachine adds state machine to a specified group
	AddStateMachineToGroup(groupIdx int, sm StateMachine)

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
	ShowMembership(groupIdx int) (NodeInfoList, error)

	//Add a paxos node to membership.
	AddMember(groupIdx int, node *NodeInfo) error

	//Remove a paxos node from membership.
	RemoveMember(groupIdx int, node *NodeInfo) error

	//Change membership by one node to another node.
	ChangeMember(groupIdx int, from, to *NodeInfo) error

	//Master
	//Check who is master.
	GetMaster(groupIdx int) *NodeInfo

	//Check who is master and get version.
	GetMasterWithVersion(groupIdx int) (n *NodeInfo, version uint64)

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
	GetInstanceValue(groupIdx int, instanceId uint64) ([]pair, error)

	OnReceiveMessage(msg []byte) error
}

type pair struct {
	s []byte
	i int64
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
	Id               uint64
	logStorage       *multiDatabase
	network          *dfNetwork
	notifyPool       *notifierPool
	groupList        []*group
	masterList       []*masterMgr
	proposeBatchList []*proposeBatch
}

func newNode() *node {
	ret := &node{}
	ret.Id = nullNode
	ret.logStorage = newMultiDatabase()
	ret.notifyPool = newNotifierPool()
	ret.network = newDFNetwork(ret)

	return ret
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

func (n *node) runProposeBatch() {
	for _, p := range n.proposeBatchList {
		p.start()
	}
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

func (n *node) init(opt *Options) error {
	err := n.checkOptions(opt)
	if err != nil {
		lPLErr("CheckOptions fail, error: %v", err)
		return err
	}

	n.Id = opt.MyNode.GetNodeID()

	//step1 init log storage
	ls, err := n.initLogStorage(opt)
	if err != nil {
		return err
	}

	//step2 init network
	nw, err := n.initNetwork(opt)
	if err != nil {
		return err
	}

	//step3 build masterList
	for groupIdx := 0; groupIdx < opt.GroupCount; groupIdx++ {
		masterMgr := newMasterMgr(n, groupIdx, ls)
		n.masterList = append(n.masterList, masterMgr)

		err := masterMgr.init()
		if err != nil {
			lPLErr("initLogStorage fail, error: %v", err)
			return err
		}
	}

	//step4 build groupList
	for groupIdx := 0; groupIdx < opt.GroupCount; groupIdx++ {
		group := newGroup(ls, nw, n.masterList[groupIdx].getMasterSM(), groupIdx, opt)
		n.groupList = append(n.groupList, group)
	}

	//step5 build batchPropose
	if opt.IsUseBatchPropose {
		for groupIdx := 0; groupIdx < opt.GroupCount; groupIdx++ {
			proposeBatch := newProposeBatch(groupIdx, n, n.notifyPool)
			n.proposeBatchList = append(n.proposeBatchList, proposeBatch)
		}
	}

	//step6 init state machine
	n.initStateMachine(opt)

	//step7 parallel init group
	for _, g := range n.groupList {
		g.startInit()
	}

	for _, g := range n.groupList {
		initErr := g.getInitRet()
		if initErr != nil {
			err = initErr
		}
	}

	if err != nil {
		lPLErr("init group fail, error: %v", err)
		return err
	}

	//last step. must init ok, then should start threads.
	//because that stop threads is slower, if init fail, we need much time to stop many threads.
	//so we put start threads in the last step.
	for _, g := range n.groupList {
		g.start()
	}

	n.runMaster(opt)
	n.runProposeBatch()

	nw.RunNetWork()
	lPLHead("OK")

	return nil
}

func (n *node) checkGroupID(groupIdx int) bool {
	if groupIdx < 0 || groupIdx >= len(n.groupList) {
		return false
	}

	return true
}

func (n *node) Propose(groupIdx int, value []byte, ctx *SMCtx) (uint64, error) {
	if !n.checkGroupID(groupIdx) {
		return 0, errGroupIdxWrong
	}
	instanceID, ret := n.groupList[groupIdx].getCommitter().newValueGetID(value, ctx)
	return instanceID, RetErrMap[ret]
}

func (n *node) GetNowInstanceID(groupIdx int) uint64 {
	if !n.checkGroupID(groupIdx) {
		return math.MaxUint64
	}

	return n.groupList[groupIdx].getInstance().getNowInstanceID()
}

func (n *node) OnReceiveMessage(msg []byte) error {
	if len(msg) == 0 {
		lPLErr("Message size %d to small, not valid.", len(msg))
		return errMsgSizeTooSmall
	}

	groupIdx := int(binary.LittleEndian.Uint64(msg))

	if !n.checkGroupID(groupIdx) {
		lPLErr("Message groupid %d wrong, groupsize %d", groupIdx, len(n.groupList))
		return errGroupIdxWrong
	}

	return n.groupList[groupIdx].getInstance().onReceiveMessage(msg)
}

func (n *node) AddStateMachine(sm StateMachine) {
	for _, g := range n.groupList {
		g.addStateMachine(sm)
	}
}

func (n *node) AddStateMachineToGroup(groupIdx int, sm StateMachine) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	n.groupList[groupIdx].addStateMachine(sm)
}

func (n *node) GetMyNodeID() uint64 {
	return n.Id
}

func (n *node) SetTimeoutMs(timeoutMs int) {
	for _, g := range n.groupList {
		g.getCommitter().setTimeoutMs(timeoutMs)
	}
}

func (n *node) SetHoldPaxosLogCount(holdCount uint64) {
	for _, g := range n.groupList {
		g.getCheckpointCleaner().setHoldPaxosLogCount(holdCount)
	}
}

func (n *node) PauseCheckpointRePlayer() {
	for _, g := range n.groupList {
		g.getCheckpointRePlayer().pause()
	}
}

func (n *node) ContinueCheckpointRePlayer() {
	for _, g := range n.groupList {
		g.getCheckpointRePlayer().resume()
	}
}

func (n *node) PausePaxosLogCleaner() {
	for _, g := range n.groupList {
		g.getCheckpointCleaner().pause()
	}
}

func (n *node) ContinuePaxosLogCleaner() {
	for _, g := range n.groupList {
		g.getCheckpointCleaner().resume()
	}
}

func (n *node) proposalMembership(sysVSM *systemVSM, groupIdx int, list NodeInfoList, version uint64) error {
	value, err := sysVSM.membershipOPValue(list, version)
	if err != nil {
		return errPaxosSystemError
	}

	ctx := &SMCtx{}

	var smRet error
	ctx.SMID = system_V_SMID
	ctx.Ctx = &smRet

	_, err = n.Propose(groupIdx, value, ctx)
	if err != nil {
		return err
	}

	return smRet
}

func (n *node) AddMember(groupIdx int, node *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return errGroupIdxWrong
	}

	sysVSM := n.groupList[groupIdx].getConfig().getSystemVSM()

	if sysVSM.getGid() == 0 {
		return errMembershipOpNoGid
	}

	nodeList, version := sysVSM.getMembership()

	for _, v := range nodeList {
		if v.GetNodeID() == node.GetNodeID() {
			return errMembershipOpNodeExists
		}
	}

	nodeList = append(nodeList, node)

	return n.proposalMembership(sysVSM, groupIdx, nodeList, version)
}

func (n *node) RemoveMember(groupIdx int, node *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return errGroupIdxWrong
	}

	sysVSM := n.groupList[groupIdx].getConfig().getSystemVSM()
	if sysVSM.getGid() == 0 {
		return errMembershipOpNoGid
	}

	nodeList, version := sysVSM.getMembership()

	var nodeExist bool
	afterNodeList := make(NodeInfoList, 0, len(nodeList))

	for _, v := range nodeList {
		if v.GetNodeID() == node.GetNodeID() {
			nodeExist = true
		} else {
			afterNodeList = append(afterNodeList, v)
		}
	}

	if !nodeExist {
		return errMembershipOpRemoveNodeNotExists
	}

	return n.proposalMembership(sysVSM, groupIdx, afterNodeList, version)
}

func (n *node) ChangeMember(groupIdx int, from, to *NodeInfo) error {
	if !n.checkGroupID(groupIdx) {
		return errGroupIdxWrong
	}

	sysVSM := n.groupList[groupIdx].getConfig().getSystemVSM()
	if sysVSM.getGid() == 0 {
		return errMembershipOpNoGid
	}

	nodeList, version := sysVSM.getMembership()

	var fromNodeExist bool
	var toNodeExist bool
	afterNodeList := make(NodeInfoList, 0, len(nodeList))

	for _, v := range nodeList {
		if v.GetNodeID() == from.GetNodeID() {
			fromNodeExist = true
			continue
		}

		if v.GetNodeID() == to.GetNodeID() {
			toNodeExist = true
			continue
		}

		afterNodeList = append(afterNodeList, v)
	}

	if !fromNodeExist && !toNodeExist {
		return errMembershipOpNoChange
	}

	afterNodeList = append(afterNodeList, to)

	return n.proposalMembership(sysVSM, groupIdx, afterNodeList, version)
}

func (n *node) ShowMembership(groupIdx int) (NodeInfoList, error) {
	if !n.checkGroupID(groupIdx) {
		return nil, errGroupIdxWrong
	}

	sysVSM := n.groupList[groupIdx].getConfig().getSystemVSM()

	nodeList, _ := sysVSM.getMembership()

	return nodeList, nil
}

func (n *node) GetMaster(groupIdx int) *NodeInfo {
	if !n.checkGroupID(groupIdx) {
		return nil
	}
	nid := n.masterList[groupIdx].getMasterSM().getMaster()
	nodeInfo := NewNodeInfo(nid, "", 0)
	nodeInfo.parseNodeID()
	return nodeInfo
}

func (n *node) GetMasterWithVersion(groupIdx int) (*NodeInfo, uint64) {
	if !n.checkGroupID(groupIdx) {
		return nil, 0
	}

	nid, version := n.masterList[groupIdx].getMasterSM().getMasterWithVersion()
	nodeInfo := NewNodeInfo(nid, "", 0)
	nodeInfo.parseNodeID()

	return nodeInfo, version
}

func (n *node) IsIMMaster(groupIdx int) bool {
	if !n.checkGroupID(groupIdx) {
		return false
	}

	return n.masterList[groupIdx].getMasterSM().isIMMaster()
}

func (n *node) SetMasterLease(groupIdx int, leaseTimeMs int) error {
	if !n.checkGroupID(groupIdx) {
		return errGroupIdxWrong
	}

	n.masterList[groupIdx].setLeaseTime(leaseTimeMs)
	return nil
}

func (n *node) DropMaster(groupIdx int) error {
	if !n.checkGroupID(groupIdx) {
		return errGroupIdxWrong
	}

	n.masterList[groupIdx].dropMaster()
	return nil
}

func (n *node) SetMaxHoldThreads(groupIdx int, maxHoldThreads int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	n.groupList[groupIdx].getCommitter().setMaxHoldThreads(maxHoldThreads)
}

func (n *node) SetProposeWaitTimeThresholdMS(groupIdx int, waitTimeThresholdMS int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	n.groupList[groupIdx].getCommitter().setProposeWaitTimeThresholdMs(waitTimeThresholdMS)
}

func (n *node) SetLogSync(groupIdx int, bLogSync bool) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	n.groupList[groupIdx].getConfig().setLogSync(bLogSync)
}

func (n *node) GetInstanceValue(groupIdx int, instanceId uint64) ([]pair, error) {
	if !n.checkGroupID(groupIdx) {
		return nil, errGroupIdxWrong
	}

	value, smID, err := n.groupList[groupIdx].getInstance().getInstanceValue(instanceId)
	if err != nil {
		return nil, err
	}

	ret := make([]pair, 0, 5)
	if smID == batch_Propose_SMID {
		batchValue := &paxospb.BatchPaxosValues{}
		err = batchValue.Unmarshal(value)
		if err != nil {
			return nil, errPaxosSystemError
		}

		for _, v := range batchValue.GetValues() {
			ret = append(ret, pair{
				s: v.GetValue(),
				i: v.GetSMID(),
			})
		}
	} else {
		ret = append(ret, pair{
			s: value,
			i: smID,
		})
	}

	return ret, nil
}

func (n *node) BatchPropose(groupIdx int, value []byte, smCtx *SMCtx) (uint64, int, error) {
	if !n.checkGroupID(groupIdx) {
		return 0, 0, errGroupIdxWrong
	}

	if len(n.proposeBatchList) == 0 {
		return 0, 0, errPaxosSystemError
	}
	var instanceID uint64
	var batchIdx int32
	err := n.proposeBatchList[groupIdx].propose(value, &instanceID, &batchIdx, smCtx)

	return instanceID, int(batchIdx), err
}

func (n *node) SetBatchCount(groupIdx int, batchCount int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	if len(n.proposeBatchList) == 0 {
		return
	}

	n.proposeBatchList[groupIdx].setBatchCount(batchCount)
}

func (n *node) SetBatchDelayTimeMs(groupIdx int, batchDelayTimeMs int) {
	if !n.checkGroupID(groupIdx) {
		return
	}

	if len(n.proposeBatchList) == 0 {
		return
	}

	n.proposeBatchList[groupIdx].setBatchDelayTimeMs(batchDelayTimeMs)
}

func (n *node) stop() {
	//1.step: must stop master(app) f irst.
	for _, m := range n.masterList {
		m.stopMaster()
	}

	//2.step: stop propose batch
	for _, p := range n.proposeBatchList {
		p.stop()
	}

	//3.step: stop network.
	n.network.StopNetWork()

	//4.step: delete paxos instance. 析构
	for _, g := range n.groupList {
		g.stop()
	}
}
