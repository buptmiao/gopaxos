package gopaxos

import (
	"github.com/buptmiao/gopaxos/paxospb"
)

type acceptor struct {
	base
	state *acceptorState
}

func newAcceptor(conf *config, tran MsgTransport, i *instance, ls LogStorage) *acceptor {
	ret := &acceptor{}
	ret.base = newBase(conf, tran, i)
	ret.state = newAcceptorState(conf, ls)

	return ret
}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////
func (a *acceptor) newInstance() {
	a.instanceID++
	a.initForNewPaxosInstance()
}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////

func (a *acceptor) init() error {
	instanceID, err := a.state.load()
	if err != nil {
		lNLErr("Load State fail, error: %v", err)
		return err
	}

	if instanceID == 0 {
		lPLGImp(a.conf.groupIdx, "Empty database")
	}

	a.setInstanceID(instanceID)

	lPLGImp(a.conf.groupIdx, "OK")

	return nil
}

func (a *acceptor) initForNewPaxosInstance() {
	a.state.init()
}

func (a *acceptor) getAcceptorState() *acceptorState {
	return a.state
}

func (a *acceptor) onPrepare(paxosMsg *paxospb.PaxosMsg) error {
	lPLGHead(a.conf.groupIdx, "START Msg.InstanceID %d Msg.from_nodeid %d Msg.ProposalID %d",
		paxosMsg.GetInstanceID(), paxosMsg.GetNodeID(), paxosMsg.GetProposalID())

	getBPInstance().OnPrepare()

	replyPaxosMsg := &paxospb.PaxosMsg{}
	replyPaxosMsg.InstanceID = a.getInstanceID()
	replyPaxosMsg.NodeID = a.conf.getMyNodeID()
	replyPaxosMsg.ProposalID = paxosMsg.GetProposalID()
	replyPaxosMsg.MsgType = msgType_PaxosPrepareReply

	ballot := newBallotNumber(paxosMsg.GetProposalID(), paxosMsg.GetNodeID())

	if ballot.gte(a.state.getPromiseBallot()) {
		lPLGDebug(a.conf.groupIdx, "[Promise] State.PromiseID %d State.PromiseNodeID %d "+
			"State.PreAcceptedID %d State.PreAcceptedNodeID %d",
			a.state.getPromiseBallot().proposalID,
			a.state.getPromiseBallot().nodeID,
			a.state.getAcceptedBallot().proposalID,
			a.state.getAcceptedBallot().nodeID)

		replyPaxosMsg.PreAcceptID = a.state.getAcceptedBallot().proposalID
		replyPaxosMsg.PreAcceptNodeID = a.state.getAcceptedBallot().nodeID

		if a.state.getAcceptedBallot().proposalID > 0 {
			replyPaxosMsg.Value = a.state.getAcceptedValue()
		}

		a.state.setPromiseBallot(ballot)

		if err := a.state.persist(a.getInstanceID(), a.getLastChecksum()); err != nil {
			getBPInstance().OnPreparePersistFail()
			lPLGErr(a.conf.groupIdx, "Persist fail, Now.InstanceID %d ,error: %v",
				a.getInstanceID(), err)
			return err
		}

		getBPInstance().OnPreparePass()

	} else {
		getBPInstance().OnPrepareReject()

		lPLGDebug(a.conf.groupIdx, "[Reject] State.PromiseID %d State.PromiseNodeID %d",
			a.state.getPromiseBallot().proposalID,
			a.state.getPromiseBallot().nodeID)
		replyPaxosMsg.RejectByPromiseID = a.state.getPromiseBallot().proposalID
	}

	replyNodeID := paxosMsg.GetNodeID()

	lPLGHead(a.conf.groupIdx, "END Now.InstanceID %d ReplyNodeID %d",
		a.getInstanceID(), paxosMsg.GetNodeID())

	a.sendPaxosMessage(replyNodeID, replyPaxosMsg, UDP)

	return nil
}

func (a *acceptor) onAccept(paxosMsg *paxospb.PaxosMsg) {
	lPLGHead(a.conf.groupIdx, "START Msg.InstanceID %d Msg.from_nodeid %d Msg.ProposalID %d Msg.ValueLen %d",
		paxosMsg.GetInstanceID(), paxosMsg.GetNodeID(), paxosMsg.GetProposalID(), len(paxosMsg.GetValue()))

	getBPInstance().OnAccept()

	replyPaxosMsg := &paxospb.PaxosMsg{}
	replyPaxosMsg.InstanceID = a.getInstanceID()
	replyPaxosMsg.NodeID = a.conf.getMyNodeID()
	replyPaxosMsg.ProposalID = paxosMsg.GetProposalID()
	replyPaxosMsg.MsgType = msgType_PaxosAcceptReply

	ballot := newBallotNumber(paxosMsg.GetProposalID(), paxosMsg.GetNodeID())
	if ballot.gte(a.state.getPromiseBallot()) {
		lPLGDebug(a.conf.groupIdx, "[Promise] State.PromiseID %d State.PromiseNodeID %d "+
			"State.PreAcceptedID %d State.PreAcceptedNodeID %d",
			a.state.getPromiseBallot().proposalID,
			a.state.getPromiseBallot().nodeID,
			a.state.getAcceptedBallot().proposalID,
			a.state.getAcceptedBallot().nodeID)

		a.state.setPromiseBallot(ballot)
		a.state.setAcceptedBallot(ballot)
		a.state.setAcceptedValue(paxosMsg.GetValue())

		if err := a.state.persist(a.getInstanceID(), a.getLastChecksum()); err != nil {

			getBPInstance().OnAcceptPersistFail()

			lPLGErr(a.conf.groupIdx, "Persist fail, Now.InstanceID %d ,error: %v",
				a.getInstanceID(), err)

			return
		}

		getBPInstance().OnAcceptPass()

	} else {
		getBPInstance().OnAcceptReject()

		lPLGDebug(a.conf.groupIdx, "[Reject] State.PromiseID %d State.PromiseNodeID %d",
			a.state.getPromiseBallot().proposalID,
			a.state.getPromiseBallot().nodeID)

		replyPaxosMsg.RejectByPromiseID = a.state.getPromiseBallot().proposalID
	}

	replyNodeID := paxosMsg.GetNodeID()

	lPLGHead(a.conf.groupIdx, "END Now.InstanceID %d ReplyNodeID %d",
		a.getInstanceID(), paxosMsg.GetNodeID())

	a.sendPaxosMessage(replyNodeID, replyPaxosMsg, UDP)
}

///////////////////////////////////////////////////////////////////////////////

type acceptorState struct {
	promiseBallot  *ballotNumber
	acceptedBallot *ballotNumber
	acceptedValue  []byte
	checksum       uint32
	conf           *config
	paxosLog       *paxosLog
	syncTimes      int
}

func newAcceptorState(conf *config, ls LogStorage) *acceptorState {
	ret := &acceptorState{}
	ret.paxosLog = newPaxosLog(ls)
	ret.syncTimes = 0
	ret.conf = conf
	ret.init()

	return ret
}

func (a *acceptorState) init() {
	a.acceptedBallot.reset()
	a.acceptedValue = nil
	a.checksum = 0
}

func (a *acceptorState) getPromiseBallot() *ballotNumber {
	return a.promiseBallot
}

func (a *acceptorState) setPromiseBallot(b *ballotNumber) {
	a.promiseBallot = b
}

func (a *acceptorState) getAcceptedBallot() *ballotNumber {
	return a.acceptedBallot
}

func (a *acceptorState) setAcceptedBallot(b *ballotNumber) {
	a.acceptedBallot = b
}

func (a *acceptorState) getAcceptedValue() []byte {
	return a.acceptedValue
}

func (a *acceptorState) setAcceptedValue(value []byte) {
	a.acceptedValue = value
}

func (a *acceptorState) getChecksum() uint32 {
	return a.checksum
}

func (a *acceptorState) persist(instanceID uint64, lastChecksum uint32) error {
	if instanceID > 0 && lastChecksum == 0 {
		a.checksum = 0
	} else if len(a.getAcceptedValue()) > 0 {
		a.checksum = crc(lastChecksum, a.getAcceptedValue())
	}

	state := &paxospb.AcceptorStateData{}

	state.InstanceID = instanceID
	state.PromiseID = a.promiseBallot.proposalID
	state.PromiseNodeID = a.promiseBallot.nodeID
	state.AcceptedID = a.acceptedBallot.proposalID
	state.AcceptedNodeID = a.acceptedBallot.nodeID
	state.AcceptedValue = a.acceptedValue
	state.Checksum = a.checksum

	wo := writeOptions(a.conf.getLogSync())

	if wo {
		a.syncTimes++
		if a.syncTimes > a.conf.getSyncInterval() {
			a.syncTimes = 0
		} else {
			wo = false
		}
	}

	if err := a.paxosLog.writeState(wo, a.conf.getMyGroupIdx(), instanceID, state); err != nil {
		return err
	}

	lPLGImp(a.conf.groupIdx, "GroupIdx %d InstanceID %d PromiseID %d PromiseNodeID %d "+
		"AccectpedID %d AcceptedNodeID %d ValueLen %d Checksum %d",
		a.conf.getMyGroupIdx(), instanceID, a.promiseBallot.proposalID,
		a.promiseBallot.nodeID, a.acceptedBallot.proposalID,
		a.acceptedBallot.nodeID, len(a.acceptedValue), a.checksum)

	return nil
}

func (a *acceptorState) load() (uint64, error) {
	instanceID, err := a.paxosLog.getMaxInstanceIDFromLog(a.conf.groupIdx)
	if err != nil && err != errMaxInstanceIDNotExist {
		lPLGErr(a.conf.groupIdx, "Load max instance id fail, error: %v", err)
		return 0, err
	}

	if err == errMaxInstanceIDNotExist {
		lPLGErr(a.conf.groupIdx, "empty database")
		return 0, nil
	}

	state, err := a.paxosLog.readState(a.conf.groupIdx, instanceID)
	if err != nil {
		return 0, err
	}

	a.promiseBallot.proposalID = state.GetPromiseID()
	a.promiseBallot.nodeID = state.GetPromiseNodeID()
	a.acceptedBallot.proposalID = state.GetAcceptedID()
	a.acceptedBallot.nodeID = state.GetAcceptedNodeID()
	a.acceptedValue = state.GetAcceptedValue()
	a.checksum = state.GetChecksum()

	lPLGImp(a.conf.groupIdx, "GroupIdx %d InstanceID %d PromiseID %d PromiseNodeID %d"+
		" AccectpedID %d AcceptedNodeID %d ValueLen %d Checksum %d",
		a.conf.getMyGroupIdx(), instanceID, a.promiseBallot.proposalID,
		a.promiseBallot.nodeID, a.acceptedBallot.proposalID,
		a.acceptedBallot.nodeID, len(a.acceptedValue), a.checksum)

	return instanceID, nil
}
