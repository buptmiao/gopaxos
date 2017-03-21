package gopaxos

import (
	"github.com/buptmiao/gopaxos/paxospb"
	"math/rand"
)

type proposer struct {
	base
	state                *proposerState
	msgCounter           *msgCounter
	learner              *learner
	isPreparing          bool
	isAccepting          bool
	loop                 *ioLoop
	prepareTimerID       uint32
	lastPrepareTimeoutMs int
	acceptTimerID        uint32
	lastAcceptTimeoutMs  int
	timeoutInstanceID    uint64
	canSkipPrepare       bool
	wasRejectBySomeone   bool
	timeStat             timeStat
}

func newProposer(conf *config, tran MsgTransport, i *instance, learner *learner, loop *ioLoop) *proposer {
	ret := &proposer{}
	ret.base = newBase(conf, tran, i)
	ret.state = newProposerState(conf)
	ret.msgCounter = newMsgCounter(conf)
	ret.learner = learner
	ret.loop = loop
	ret.initForNewPaxosInstance()
	ret.lastPrepareTimeoutMs = ret.conf.getPrepareTimeoutMs()
	ret.lastAcceptTimeoutMs = ret.conf.getAcceptTimeoutMs()

	return ret
}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////
func (p *proposer) newInstance() {
	p.instanceID++
	p.initForNewPaxosInstance()
}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////
func (p *proposer) setStartProposalID(id uint64) {
	p.state.setStartProposalID(id)
}

func (p *proposer) initForNewPaxosInstance() {
	p.msgCounter.startNewRound()
	p.state.init()

	p.exitPrepare()
	p.exitAccept()
}

func (p *proposer) isWorking() bool {
	return p.isPreparing || p.isAccepting
}

func (p *proposer) newValue(value []byte) {
	getBPInstance().NewProposal(value)

	if len(p.state.getValue()) == 0 {
		p.state.setValue(value)
	}

	p.lastPrepareTimeoutMs = getInsideOptionsInstance().getStartPrepareTimeoutMs()
	p.lastAcceptTimeoutMs = getInsideOptionsInstance().getStartAcceptTimeoutMs()

	if p.canSkipPrepare && !p.wasRejectBySomeone {
		getBPInstance().NewProposalSkipPrepare()

		lPLGHead(p.conf.groupIdx, "skip prepare, directly start accept")
		p.accept()
	} else {
		//if not reject by someone, no need to increase ballot
		p.prepare(p.wasRejectBySomeone)
	}
}

func (p *proposer) exitPrepare() {
	if p.isPreparing {
		p.isPreparing = false
		p.prepareTimerID = p.loop.removeTimer(p.prepareTimerID)
	}
}

func (p *proposer) exitAccept() {
	if p.isAccepting {
		p.isAccepting = false
		p.acceptTimerID = p.loop.removeTimer(p.acceptTimerID)
	}
}

func (p *proposer) addPrepareTimer(timeoutMs int) {
	if p.prepareTimerID > 0 {
		p.prepareTimerID = p.loop.removeTimer(p.prepareTimerID)
	}

	if timeoutMs > 0 {
		p.prepareTimerID, _ = p.loop.addTimer(timeoutMs, timer_Proposer_Prepare_Timeout)
		return
	}

	p.prepareTimerID, _ = p.loop.addTimer(p.lastPrepareTimeoutMs, timer_Proposer_Prepare_Timeout)

	p.timeoutInstanceID = p.getInstanceID()

	lPLGHead(p.conf.groupIdx, "timeoutms %d", p.lastPrepareTimeoutMs)

	p.lastPrepareTimeoutMs *= 2

	if p.lastPrepareTimeoutMs > getInsideOptionsInstance().getMaxPrepareTimeoutMs() {
		p.lastPrepareTimeoutMs = getInsideOptionsInstance().getMaxPrepareTimeoutMs()
	}
}

func (p *proposer) addAcceptTimer(timeoutMs int) {
	if p.acceptTimerID > 0 {
		p.acceptTimerID = p.loop.removeTimer(p.acceptTimerID)
	}

	if timeoutMs > 0 {
		p.acceptTimerID, _ = p.loop.addTimer(timeoutMs, timer_Proposer_Accept_Timeout)
		return
	}

	p.acceptTimerID, _ = p.loop.addTimer(p.lastAcceptTimeoutMs, timer_Proposer_Accept_Timeout)

	p.timeoutInstanceID = p.getInstanceID()

	lPLGHead(p.conf.groupIdx, "timeoutms %d", p.lastAcceptTimeoutMs)

	p.lastAcceptTimeoutMs *= 2
	if p.lastAcceptTimeoutMs > getInsideOptionsInstance().getMaxAcceptTimeoutMs() {
		p.lastAcceptTimeoutMs = getInsideOptionsInstance().getMaxAcceptTimeoutMs()
	}
}

func (p *proposer) prepare(needNewBallot bool) {
	lPLGHead(p.conf.groupIdx, "START Now.InstanceID %d MyNodeID %d State.ProposalID %d State.ValueLen %d",
		p.getInstanceID(), p.conf.getMyNodeID(), p.state.getProposalID(),
		len(p.state.getValue()))

	getBPInstance().Prepare()
	p.timeStat.point()

	p.exitAccept()
	p.isPreparing = true
	p.canSkipPrepare = false
	p.wasRejectBySomeone = false

	p.state.resetHighestOtherPreAcceptBallot()
	if needNewBallot {
		p.state.newPrepare()
	}

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.MsgType = int32(msgType_PaxosPrepare)
	paxosMsg.InstanceID = p.getInstanceID()
	paxosMsg.NodeID = p.conf.getMyNodeID()
	paxosMsg.ProposalID = p.state.getProposalID()

	p.msgCounter.startNewRound()

	p.addPrepareTimer(0)

	lPLGHead(p.conf.groupIdx, "END OK")

	p.broadcastMessage(paxosMsg, broadcastMessage_Type_RunSelf_First, UDP)
}

func (p *proposer) onPrepareReply(paxosMsg *paxospb.PaxosMsg) {
	lPLGHead(p.conf.groupIdx, "START Msg.ProposalID %d State.ProposalID %d Msg.from_nodeid %d RejectByPromiseID %d",
		paxosMsg.GetProposalID(), p.state.getProposalID(),
		paxosMsg.GetNodeID(), paxosMsg.GetRejectByPromiseID())

	getBPInstance().OnPrepareReply()

	if !p.isPreparing {
		getBPInstance().OnPrepareReplyButNotPreparing()

		return
	}

	if paxosMsg.GetProposalID() != p.state.getProposalID() {
		getBPInstance().OnPrepareReplyNotSameProposalIDMsg()

		return
	}
	p.msgCounter.addReceive(paxosMsg.GetNodeID())

	if paxosMsg.GetRejectByPromiseID() == 0 {
		ballot := newBallotNumber(paxosMsg.GetPreAcceptID(), paxosMsg.GetPreAcceptNodeID())
		lPLGDebug(p.conf.groupIdx, "[Promise] PreAcceptedID %d PreAcceptedNodeID %d ValueSize %d",
			paxosMsg.GetPreAcceptID(), paxosMsg.GetPreAcceptNodeID(), len(paxosMsg.GetValue()))

		p.msgCounter.addPromiseOrAccept(paxosMsg.GetNodeID())
		p.state.addPreAcceptValue(ballot, paxosMsg.GetValue())

	} else {
		lPLGDebug(p.conf.groupIdx, "[Reject] RejectByPromiseID %d", paxosMsg.GetRejectByPromiseID())
		p.msgCounter.addReject(paxosMsg.GetNodeID())
		p.wasRejectBySomeone = true
		p.state.setOtherProposalID(paxosMsg.GetRejectByPromiseID())
	}

	if p.msgCounter.isPassedOnThisRound() {
		useTimeMs := p.timeStat.point()
		getBPInstance().PreparePass(useTimeMs)

		lPLGImp(p.conf.groupIdx, "[Pass] start accept, usetime %dms", useTimeMs)
		p.canSkipPrepare = true
		p.accept()
	} else if p.msgCounter.isRejectedOnThisRound() || p.msgCounter.isAllReceiveOnThisRound() {
		getBPInstance().PrepareNotPass()
		lPLGImp(p.conf.groupIdx, "[Not Pass] wait 30ms and restart prepare")
		p.addPrepareTimer(int(rand.Uint32()%30 + 10))
	}

	lPLGHead(p.conf.groupIdx, "END")
}

func (p *proposer) onExpirePrepareReply(paxosMsg *paxospb.PaxosMsg) {
	if paxosMsg.GetRejectByPromiseID() != 0 {
		lPLGDebug(p.conf.groupIdx, "[Expired Prepare Reply Reject] RejectByPromiseID %d", paxosMsg.GetRejectByPromiseID())
		p.wasRejectBySomeone = true
		p.state.setOtherProposalID(paxosMsg.GetRejectByPromiseID())
	}
}

func (p *proposer) accept() {
	lPLGHead(p.conf.groupIdx, "START ProposalID %d ValueSize %d ValueLen %d",
		p.state.getProposalID(), len(p.state.getValue()), len(p.state.getValue()))

	getBPInstance().Accept()

	p.timeStat.point()
	p.exitPrepare()
	p.isAccepting = true

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.MsgType = int32(msgType_PaxosAccept)
	paxosMsg.InstanceID = p.getInstanceID()
	paxosMsg.NodeID = p.conf.getMyNodeID()
	paxosMsg.ProposalID = p.state.getProposalID()
	paxosMsg.Value = p.state.getValue()
	paxosMsg.LastChecksum = p.getLastChecksum()

	p.msgCounter.startNewRound()

	p.addAcceptTimer(0)

	lPLGHead(p.conf.groupIdx, "END")

	p.broadcastMessage(paxosMsg, broadcastMessage_Type_RunSelf_Final, UDP)
}

func (p *proposer) onAcceptReply(paxosMsg *paxospb.PaxosMsg) {
	lPLGHead(p.conf.groupIdx, "START Msg.ProposalID %d State.ProposalID %d Msg.from_nodeid %d RejectByPromiseID %d",
		paxosMsg.GetProposalID(), p.state.getProposalID(),
		paxosMsg.GetNodeID(), paxosMsg.GetRejectByPromiseID())

	getBPInstance().OnAcceptReply()

	if !p.isAccepting {
		getBPInstance().OnAcceptReplyButNotAccepting()
		return
	}

	if paxosMsg.GetProposalID() != p.state.getProposalID() {
		getBPInstance().OnAcceptReplyNotSameProposalIDMsg()
		return
	}

	p.msgCounter.addReceive(paxosMsg.GetNodeID())

	if paxosMsg.GetRejectByPromiseID() == 0 {
		lPLGDebug(p.conf.groupIdx, "[Accept]")
		p.msgCounter.addPromiseOrAccept(paxosMsg.GetNodeID())
	} else {
		lPLGDebug(p.conf.groupIdx, "[Reject]")
		p.msgCounter.addReject(paxosMsg.GetNodeID())
		p.wasRejectBySomeone = true
		p.state.setOtherProposalID(paxosMsg.GetRejectByPromiseID())
	}

	if p.msgCounter.isPassedOnThisRound() {
		useTimeMs := p.timeStat.point()
		getBPInstance().AcceptPass(useTimeMs)

		lPLGImp(p.conf.groupIdx, "[Pass] Start send learn, usetime %dms", useTimeMs)
		p.exitAccept()
		p.learner.proposerSendSuccess(p.getInstanceID(), p.state.getProposalID())
	} else if p.msgCounter.isRejectedOnThisRound() || p.msgCounter.isAllReceiveOnThisRound() {
		getBPInstance().AcceptNotPass()

		lPLGImp(p.conf.groupIdx, "[Not pass] wait 30ms and Restart prepare")
		p.addAcceptTimer(int(rand.Uint32()%30 + 10))
	}

	lPLGHead(p.conf.groupIdx, "END")
}

func (p *proposer) onExpiredAcceptReply(paxosMsg *paxospb.PaxosMsg) {
	if paxosMsg.GetRejectByPromiseID() != 0 {
		lPLGDebug(p.conf.groupIdx, "[Expired Accept Reply Reject] RejectByPromiseID %d", paxosMsg.GetRejectByPromiseID())
		p.wasRejectBySomeone = true
		p.state.setOtherProposalID(paxosMsg.GetRejectByPromiseID())
	}
}

func (p *proposer) onPrepareTimeout() {
	lPLGHead(p.conf.groupIdx, "OK")

	if p.getInstanceID() != p.timeoutInstanceID {
		lPLGErr(p.conf.groupIdx, "TimeoutInstanceID %d not same to NowInstanceID %d, skip",
			p.timeoutInstanceID, p.getInstanceID())

		return
	}

	getBPInstance().PrepareTimeout()

	p.prepare(p.wasRejectBySomeone)
}

func (p *proposer) onAcceptTimeout() {
	lPLGHead(p.conf.groupIdx, "OK")

	if p.getInstanceID() != p.timeoutInstanceID {
		lPLGErr(p.conf.groupIdx, "TimeoutInstanceID %d not same to NowInstanceID %d, skip",
			p.timeoutInstanceID, p.getInstanceID())

		return
	}

	getBPInstance().AcceptTimeout()

	p.prepare(p.wasRejectBySomeone)
}

func (p *proposer) cancelSkipPrepare() {
	p.canSkipPrepare = false
}

///////////////////////////////////////////////////////////////////////////////
// proposerState
///////////////////////////////////////////////////////////////////////////////
type proposerState struct {
	proposalID                  uint64
	highestOtherProposalID      uint64
	value                       []byte
	highestOtherPreAcceptBallot *ballotNumber
	conf                        *config
}

func newProposerState(conf *config) *proposerState {
	ret := &proposerState{}
	ret.conf = conf
	ret.proposalID = 1
	ret.init()

	return ret
}

func (p *proposerState) init() {
	p.highestOtherProposalID = 0
	p.highestOtherPreAcceptBallot = newBallotNumber(0, 0)
	p.value = nil
}

func (p *proposerState) setStartProposalID(proposalID uint64) {
	p.proposalID = proposalID
}

func (p *proposerState) newPrepare() {
	lPLGHead(p.conf.groupIdx, "START ProposalID %d HighestOther %d MyNodeID %d",
		p.proposalID, p.highestOtherProposalID, p.conf.getMyNodeID())

	maxProposalID := p.highestOtherProposalID
	if p.proposalID > p.highestOtherProposalID {
		maxProposalID = p.proposalID
	}

	p.proposalID = maxProposalID + 1
	lPLGHead(p.conf.groupIdx, "END New.ProposalID %d", p.proposalID)
}

func (p *proposerState) addPreAcceptValue(otherPreAcceptBallot *ballotNumber, otherPreAcceptValue []byte) {
	lPLGDebug(p.conf.groupIdx, "OtherPreAcceptID %d OtherPreAcceptNodeID %d HighestOtherPreAcceptID %d "+
		"HighestOtherPreAcceptNodeID %d OtherPreAcceptValue %d",
		otherPreAcceptBallot.proposalID, otherPreAcceptBallot.nodeID,
		p.highestOtherPreAcceptBallot.proposalID, p.highestOtherPreAcceptBallot.nodeID,
		len(otherPreAcceptValue))

	if otherPreAcceptBallot.isnull() {
		return
	}

	if otherPreAcceptBallot.gt(p.highestOtherPreAcceptBallot) {
		p.highestOtherPreAcceptBallot = otherPreAcceptBallot
		p.value = otherPreAcceptValue
	}
}

func (p *proposerState) getProposalID() uint64 {
	return p.proposalID
}

func (p *proposerState) getValue() []byte {
	return p.value
}

func (p *proposerState) setValue(value []byte) {
	p.value = value
}

func (p *proposerState) setOtherProposalID(otherProposalID uint64) {
	if otherProposalID > p.highestOtherProposalID {
		p.highestOtherProposalID = otherProposalID
	}
}

func (p *proposerState) resetHighestOtherPreAcceptBallot() {
	p.highestOtherPreAcceptBallot.reset()
}
