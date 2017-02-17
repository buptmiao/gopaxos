package paxos

import (
	"math/rand"
	"time"
)

type masterMgr struct {
	paxosNode      Node
	dfMasterSM     *masterStateMachine
	leaseTime      int //unit: millisecond
	isEnd          bool
	isStarted      bool
	groupIdx       int
	needDropMaster bool
	quit           chan struct{}
}

func newMasterMgr(paxosNode Node, groupIdx int, ls LogStorage) *masterMgr {
	ret := &masterMgr{
		paxosNode:      paxosNode,
		dfMasterSM:     newMasterStateMachine(ls, paxosNode.GetMyNodeID(), groupIdx),
		leaseTime:      10000,
		groupIdx:       groupIdx,
		isEnd:          false,
		isStarted:      false,
		needDropMaster: false,
	}

	return ret
}

func (m *masterMgr) init() error {
	return m.dfMasterSM.init()
}

func (m *masterMgr) setLeaseTime(leaseTimeMs int) {
	if leaseTimeMs < 1000 {
		return
	}

	m.leaseTime = leaseTimeMs
}

func (m *masterMgr) dropMaster() {
	m.needDropMaster = true
}

func (m *masterMgr) stopMaster() {
	if m.isStarted {
		m.isEnd = true
		<-m.quit
	}
}

func (m *masterMgr) runMaster() {
	m.quit = make(chan struct{}, 1)
	go m.run()
}

func (m *masterMgr) run() {
	m.isStarted = true

	for {
		if m.isEnd {
			m.quit <- struct{}{}
			return
		}

		leaseTime := m.leaseTime

		beginTime := getSteadyClockMS()

		m.tryBeMaster(leaseTime)

		continueLeaseTimeout := (leaseTime - 100) / 4
		continueLeaseTimeout = continueLeaseTimeout/2 + rand.Uint32()%continueLeaseTimeout

		if m.needDropMaster {
			getBPInstance().DropMaster()
			m.needDropMaster = false
			continueLeaseTimeout = leaseTime * 2
			lPLG1Imp(m.groupIdx, "Need drop master, this round wait time %dms", continueLeaseTimeout)
		}

		endTime := getSteadyClockMS()
		var runTime, needSleepTime uint64
		if endTime > beginTime {
			runTime = endTime - beginTime
		}

		if continueLeaseTimeout > runTime {
			needSleepTime = continueLeaseTimeout - runTime
		}

		lPLG1Imp(m.groupIdx, "TryBeMaster, sleep time %dms", needSleepTime)
		time.Sleep(needSleepTime * time.Millisecond)
	}
}

func (m *masterMgr) tryBeMaster(leaseTime int) {
	//step 1 check exist master and get version
	masterNodeID, masterVersion := m.dfMasterSM.safeGetMaster()

	if masterNodeID != nullNode && masterNodeID != m.paxosNode.GetMyNodeID() {
		lPLG1Imp(m.groupIdx, "Ohter as master, can't try be master, masterid %d myid %d",
			masterNodeID, m.paxosNode.GetMyNodeID())
		return
	}

	getBPInstance().TryBeMaster()

	//step 2 try be master
	paxosValue, err := makeOpValue(m.paxosNode.GetMyNodeID(), masterVersion, leaseTime, masterOperatorType_Complete)
	if err != nil {
		lPLG1Err(m.groupIdx, "Make paxos value fail")
		return
	}

	masterLeaseTimeout := leaseTime - 100

	absMasterTimeout := getSteadyClockMS() + masterLeaseTimeout

	ctx := &SMCtx{
		SMID: master_v_smid,
		Ctx:  absMasterTimeout,
	}

	_, err = m.paxosNode.Propose(m.groupIdx, paxosValue, ctx)
	if err != nil {
		getBPInstance().TryBeMasterProposeFail()
	}
}

func (m *masterMgr) getMasterSM() *masterStateMachine {
	return m.dfMasterSM
}
