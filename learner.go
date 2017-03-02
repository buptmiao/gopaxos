package gopaxos

import (
	"hash/crc32"

	"github.com/buptmiao/gopaxos/paxospb"
	"os"
	"time"
)

// learner implements the interface base.
type learner struct {
	base
	state                      learnerState
	acceptor                   *acceptor
	paxosLog                   *paxosLog
	askForLearnNoopTimerID     uint32
	loop                       *ioLoop
	highestSeenInstanceID      uint64
	highestSeenInstanceIDOwner nodeId
	isIMLearning               bool
	learnerSender              learnerSender
	lastAckInstanceID          uint64
	cpMgr                      *checkpointMgr
	smFac                      *smFac
	cpSender                   *checkpointSender
	cpReceiver                 *checkpointReceiver
}

func newLearner(conf *config, tran MsgTransport, i *instance, acceptor *acceptor, ls LogStorage,
	loop *ioLoop, cpMgr *checkpointMgr, smFac *smFac) *learner {

	ret := &learner{}
	ret.base = newBase(conf, tran, i)
	ret.state = newLearnerState(conf, ls)
	ret.paxosLog = newPaxosLog(ls)
	ret.learnerSender = newLearnerSender(conf, ret, ret.paxosLog)
	ret.cpReceiver = newCheckpointReceiver(conf, ls)
	ret.acceptor = acceptor

	ret.initForNewPaxosInstance()

	ret.askForLearnNoopTimerID = 0
	ret.loop = loop
	ret.cpMgr = cpMgr
	ret.smFac = smFac
	ret.cpSender = nil
	ret.highestSeenInstanceID = 0
	ret.highestSeenInstanceIDOwner = nullNode
	ret.isIMLearning = false
	ret.lastAckInstanceID = 0

	return ret
}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////
func (l *learner) newInstance() {
	l.instanceID++
	l.initForNewPaxosInstance()
}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////

func (l *learner) startLearnerSender() {
	l.learnerSender.start()
}

func (l *learner) isLearned() bool {
	return l.state.getIsLearned()
}

func (l *learner) getLearnValue() []byte {
	return l.state.getLearnValue()
}

func (l *learner) initForNewPaxosInstance() {
	l.state.init()
}

func (l *learner) getNewChecksum() uint32 {
	return l.state.getNewChecksum()
}

func (l *learner) isIMLatest() bool {
	return (l.getInstanceID() + 1) >= l.highestSeenInstanceID
}

func (l *learner) setInstanceID(instanceID uint64) {

}

func (l *learner) getSeenLatestInstanceID() uint64 {
	return l.highestSeenInstanceID
}

func (l *learner) setSeenInstanceID(instanceID uint64, fromNodeID nodeId) {
	if instanceID > l.highestSeenInstanceID {
		l.highestSeenInstanceID = instanceID
		l.highestSeenInstanceIDOwner = fromNodeID
	}
}

func (l *learner) resetAskForLearnNoop(timeout int) {
	if l.askForLearnNoopTimerID > 0 {
		l.loop.removeTimer(l.askForLearnNoopTimerID)
	}

	l.loop.addTimer(timeout, timer_Learner_Askforlearn_noop, l.askForLearnNoopTimerID)
}

func (l *learner) askForLearnNoop(isStart bool) {
	l.resetAskForLearnNoop(getInsideOptionsInstance().getAskforLearnInterval())

	l.isIMLearning = false
	l.cpMgr.exitCheckpointMode()

	l.askForLearn()

	if isStart {
		l.askForLearn()
	}
}

func (l *learner) askForLearn() {
	getBPInstance().AskForLearn()

	lPLGHead(l.conf.groupIdx, "START")

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.InstanceID = l.getInstanceID()
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.MsgType = msgType_PaxosLearner_AskforLearn

	if l.conf.isIMFollower() {
		//this is not proposal nodeid, just use this val to bring followto nodeid info.
		paxosMsg.ProposalNodeID = l.conf.getFollowToNodeID()
	}

	lPLGHead(l.conf.groupIdx, "END InstanceID %d MyNodeID %d", paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())

	l.broadcastMessage(paxosMsg, broadcastMessage_Type_RunSelf_None, TCP)
	l.broadcastMessageToTempNode(paxosMsg, UDP)
}

func (l *learner) onAskForLearn(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnAskForLearn()

	lPLGHead(l.conf.groupIdx, "START Msg.InstanceID %d Now.InstanceID %d Msg.from_nodeid %d MinChosenInstanceID %d",
		paxosMsg.GetInstanceID(), l.getInstanceID(), paxosMsg.GetNodeID(),
		l.cpMgr.getMinChosenInstanceID())

	l.setSeenInstanceID(paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())

	if paxosMsg.GetProposalNodeID() == l.conf.getMyNodeID() {
		//Found a node follow me.
		lPLImp(l.conf.groupIdx, "Found a node %d follow me.", paxosMsg.GetNodeID())
		l.conf.addFollowerNode(paxosMsg.GetNodeID())
	}

	if paxosMsg.GetInstanceID() >= l.getInstanceID() {
		return
	}

	if paxosMsg.GetInstanceID() >= l.cpMgr.getMinChosenInstanceID() {
		if !l.learnerSender.prepare(paxosMsg.GetInstanceID(), paxosMsg.GetNodeID()) {
			getBPInstance().OnAskForLearnGetLockFail()

			lPLGErr(l.conf.groupIdx, "LearnerSender working for others.")

			if paxosMsg.GetInstanceID() == l.getInstanceID()-1 {
				lPLGImp(l.conf.groupIdx, "InstanceID only difference one, just send this value to other.")
				//send one value
				state, err := l.paxosLog.readState(l.conf.groupIdx, paxosMsg.GetInstanceID())
				if err == nil {
					ballot := newBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())
					l.sendLearnValue(paxosMsg.GetNodeID(), paxosMsg.GetInstanceID(), ballot, state.GetAcceptedValue(), 0, false)
				}
			}

			return
		}
	}

	l.sendNowInstanceID(paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())
}

func (l *learner) sendNowInstanceID(instanceID uint64, sendNodeID nodeId) {
	getBPInstance().SendNowInstanceID()

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.InstanceID = instanceID
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.MsgType = msgType_PaxosLearner_SendNowInstanceID
	paxosMsg.NowInstanceID = l.getInstanceID()
	paxosMsg.MinChosenInstanceID = l.cpMgr.getMinChosenInstanceID()

	if l.getInstanceID()-instanceID > 50 {
		//instanceid too close not need to send vsm/master checkpoint.
		sysVarBuf, err := l.conf.getSystemVSM().GetCheckpointBuffer()
		if err == nil {
			paxosMsg.SystemVariables = sysVarBuf
		}

		var masterVarBuf []byte

		if l.conf.getMasterSM() != nil {
			masterVarBuf, err = l.conf.getMasterSM().GetCheckpointBuffer()
			if err == nil {
				paxosMsg.MasterVariables = masterVarBuf
			}
		}
	}

	l.sendPaxosMessage(sendNodeID, paxosMsg, UDP)
}

func (l *learner) onSendNowInstanceID(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnSendNowInstanceID()

	lPLGHead(l.conf.groupIdx, "START Msg.InstanceID %d Now.InstanceID %d Msg.from_nodeid %d Msg.MaxInstanceID %d systemvariables_size %d mastervariables_size %d",
		paxosMsg.GetInstanceID(), l.getInstanceID(), paxosMsg.GetNodeID(), paxosMsg.GetNowInstanceID(),
		len(paxosMsg.GetSystemVariables()), len(paxosMsg.GetMasterVariables()))

	l.setSeenInstanceID(paxosMsg.GetNowInstanceID(), paxosMsg.GetNodeID())

	sysVarChange, err := l.conf.getSystemVSM().UpdateByCheckpoint(paxosMsg.GetSystemVariables())
	if err == nil && sysVarChange {
		lPLGHead(l.conf.groupIdx, "SystemVariables changed!, all thing need to refresh, so skip this msg")
		return
	}

	if l.conf.getMasterSM() != nil {
		masterVarChange, err := l.conf.getMasterSM().UpdateByCheckpoint(paxosMsg.GetMasterVariables())
		if err == nil && masterVarChange {
			lPLGHead(l.conf.groupIdx, "MasterVariables changed!")
		}
	}

	if paxosMsg.GetInstanceID() != l.getInstanceID() {
		lPLGErr(l.conf.groupIdx, "Lag msg, skip")
		return
	}

	if paxosMsg.GetNowInstanceID() <= l.getInstanceID() {
		lPLGErr(l.conf.groupIdx, "Lag msg, skip")
		return
	}

	if paxosMsg.GetMinChosenInstanceID() > l.getInstanceID() {
		getBPInstance().NeedAskForCheckpoint()

		lPLGHead(l.conf.groupIdx, "my instanceid %d small than other's minchoseninstanceid %d, other nodeid %d",
			l.getInstanceID(), paxosMsg.GetMinChosenInstanceID(), paxosMsg.GetNodeID())

		l.askForCheckpoint(paxosMsg.GetNodeID())
	} else if !l.isIMLearning {
		l.confirmAskForLearn(paxosMsg.GetNodeID())
	}
}

func (l *learner) confirmAskForLearn(sendNodeID nodeId) {
	getBPInstance().ConfirmAskForLearn()

	lPLGHead(l.conf.groupIdx, "START")

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.InstanceID = l.getInstanceID()
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.MsgType = msgType_PaxosLearner_ConfirmAskforLearn

	lPLGHead(l.conf.groupIdx, "END InstanceID %d MyNodeID %d", l.getInstanceID(), paxosMsg.GetNodeID())
	l.sendPaxosMessage(sendNodeID, paxosMsg, UDP)

	l.isIMLearning = true
}

func (l *learner) onConfirmAskForLearn(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnConfirmAskForLearn()

	lPLGHead(l.conf.groupIdx, "START Msg.InstanceID %d Msg.from_nodeid %d", paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())

	if !l.learnerSender.confirm(paxosMsg.GetInstanceID(), paxosMsg.GetNodeID()) {
		getBPInstance().OnConfirmAskForLearnGetLockFail()

		lPLGErr(l.conf.groupIdx, "LearnerSender confirm fail, maybe is lag msg")
		return
	}

	lPLGImp(l.conf.groupIdx, "OK, success confirm")
}

func (l *learner) sendLearnValue(sendNodeID nodeId, learnInstanceID uint64, learnedBallot *ballotNumber, learnedValue []byte, checksum uint32, needAck bool) error {
	getBPInstance().SendLearnValue()

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.MsgType = msgType_PaxosLearner_SendLearnValue
	paxosMsg.InstanceID = learnInstanceID
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.ProposalNodeID = learnedBallot.nodeID
	paxosMsg.ProposalID = learnedBallot.proposalID
	paxosMsg.Value = learnedValue
	paxosMsg.LastChecksum = checksum

	if needAck {
		paxosMsg.Flag = paxosMsgFlagType_SendLearnValue_NeedAck
	}

	return l.sendPaxosMessage(sendNodeID, paxosMsg, TCP)
}

func (l *learner) onSendLearnValue(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnSendLearnValue()
	lPLGHead(l.conf.groupIdx, "START Msg.InstanceID %d Now.InstanceID %d Msg.ballot_proposalid %d Msg.ballot_nodeid %d Msg.ValueSize %d",
		paxosMsg.GetInstanceID(), l.getInstanceID(), paxosMsg.GetProposalID(),
		paxosMsg.GetNodeID(), len(paxosMsg.GetValue()))

	if paxosMsg.GetInstanceID() > l.getInstanceID() {
		lPLGDebug(l.conf.groupIdx, "[Latest Msg] i can't learn")
		return
	}

	if paxosMsg.GetInstanceID() < l.getInstanceID() {
		lPLGDebug(l.conf.groupIdx, "[Lag Msg] no need to learn")
	} else {
		//learn value
		ballot := newBallotNumber(paxosMsg.GetProposalID(), paxosMsg.GetProposalNodeID())
		err := l.state.learnValue(paxosMsg.GetInstanceID(), ballot, paxosMsg.GetValue(), l.getLastChecksum())
		if err != nil {
			lPLGErr(l.conf.groupIdx, "LearnState.LearnValue fail, error: %v", err)
			return
		}

		lPLGHead(l.conf.groupIdx, "END LearnValue OK, proposalid %d proposalid_nodeid %d valueLen %d",
			paxosMsg.GetProposalID(), paxosMsg.GetNodeID(), len(paxosMsg.GetValue()))
	}

	if paxosMsg.GetFlag() == paxosMsgFlagType_SendLearnValue_NeedAck {
		//every time' when receive valid need ack learn value, reset noop timeout.
		l.resetAskForLearnNoop(getInsideOptionsInstance().getAskforLearnInterval())
		l.sendLearnValueAck(paxosMsg.GetNodeID())
	}
}

func (l *learner) sendLearnValueAck(sendNodeID nodeId) {
	lPLGHead("START LastAck.Instanceid %d Now.Instanceid %d", l.lastAckInstanceID, l.getInstanceID())

	if l.getInstanceID() < l.lastAckInstanceID+getInsideOptionsInstance().getLearnerReceiverAckLead() {
		lPLGImp(l.conf.groupIdx, "No need to ack")
		return
	}

	getBPInstance().SendLearnValue_Ack()

	l.lastAckInstanceID = l.getInstanceID()

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.InstanceID = l.getInstanceID()
	paxosMsg.MsgType = msgType_PaxosLearner_SendLearnValue_Ack
	paxosMsg.NodeID = l.conf.getMyNodeID()

	l.sendPaxosMessage(sendNodeID, paxosMsg, UDP)

	lPLGHead(l.conf.groupIdx, "End. ok")
}

func (l *learner) onSendLearnValueAck(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnSendLearnValue_Ack()

	lPLGHead(l.conf.groupIdx, "Msg.Ack.Instanceid %d Msg.from_nodeid %d", paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())

	l.learnerSender.ack(paxosMsg.GetInstanceID(), paxosMsg.GetNodeID())
}

func (l *learner) transmitToFollower() {
	if l.conf.getMyFollowerCount() == 0 {
		return
	}

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.MsgType = msgType_PaxosLearner_SendLearnValue
	paxosMsg.InstanceID = l.getInstanceID()
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.ProposalNodeID = l.acceptor.getAcceptorState().getAcceptedBallot().nodeID
	paxosMsg.ProposalID = l.acceptor.getAcceptorState().getAcceptedBallot().proposalID
	paxosMsg.Value = l.acceptor.getAcceptorState().getAcceptedValue()
	paxosMsg.LastChecksum = l.getLastChecksum()

	l.broadcastMessageToFollower(paxosMsg, TCP)

	lPLGHead(l.conf.groupIdx, "ok")
}

func (l *learner) proposerSendSuccess(learnInstanceID uint64, proposalID uint64) {
	getBPInstance().ProposerSendSuccess()

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.MsgType = msgType_PaxosLearner_ProposerSendSuccess
	paxosMsg.InstanceID = learnInstanceID
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.ProposalID = proposalID
	paxosMsg.LastChecksum = l.getLastChecksum()

	//run self first
	l.broadcastMessage(paxosMsg, broadcastMessage_Type_RunSelf_First, UDP)
}

func (l *learner) onProposerSendSuccess(paxosMsg *paxospb.PaxosMsg) {
	getBPInstance().OnProposerSendSuccess()

	lPLGHead(l.conf.groupIdx, "START Msg.InstanceID %d Now.InstanceID %d Msg.ProposalID %d State.AcceptedID %d "+
		"State.AcceptedNodeID %d, Msg.from_nodeid %d", paxosMsg.GetInstanceID(), l.getInstanceID(), paxosMsg.GetProposalID(),
		l.acceptor.getAcceptorState().getAcceptedBallot().proposalID, l.acceptor.getAcceptorState().getAcceptedBallot().nodeID,
		paxosMsg.GetNodeID())

	if paxosMsg.GetInstanceID() != l.getInstanceID() {
		//Instance id not same, that means not in the same instance, ignored.
		lPLGDebug(l.conf.groupIdx, "InstanceID not same, skip msg")
		return
	}

	if l.acceptor.getAcceptorState().getAcceptedBallot() == nil {
		//Not accept any yet.
		getBPInstance().OnProposerSendSuccessNotAcceptYet()
		lPLGDebug(l.conf.groupIdx, "I haven't accpeted any proposal")
		return
	}

	ballot := newBallotNumber(paxosMsg.GetProposalID(), paxosMsg.GetNodeID())

	if *l.acceptor.getAcceptorState().getAcceptedBallot() != *ballot {
		//ProposalID not same, this accept value maybe not chosen value.
		lPLGDebug(l.conf.groupIdx, "ProposalBallot not same to AcceptedBallot")
		getBPInstance().OnProposerSendSuccessBallotNotSame()
		return
	}

	//learn value
	l.state.learnValueWithoutWrite(paxosMsg.GetInstanceID(), l.acceptor.getAcceptorState().getAcceptedValue(), l.acceptor.getAcceptorState().getChecksum())

	getBPInstance().OnProposerSendSuccessSuccessLearn()

	lPLGHead(l.conf.groupIdx, "END Learn value OK, value %d", len(l.acceptor.getAcceptorState().getAcceptedValue()))

	l.transmitToFollower()
}

func (l *learner) askForCheckpoint(sendNodeID nodeId) {
	lPLGHead(l.conf.groupIdx, "START")

	if err := l.cpMgr.prepareForAskForCheckpoint(sendNodeID); err != nil {
		return
	}

	paxosMsg := &paxospb.PaxosMsg{}
	paxosMsg.InstanceID = l.getInstanceID()
	paxosMsg.NodeID = l.conf.getMyNodeID()
	paxosMsg.MsgType = msgType_PaxosLearner_AskforCheckpoint

	lPLGHead(l.conf.groupIdx, "END InstanceID %d MyNodeID %d", l.getInstanceID(), paxosMsg.GetNodeID())

	l.sendPaxosMessage(sendNodeID, paxosMsg, UDP)
}

func (l *learner) onAskForCheckpoint(paxosMsg *paxospb.PaxosMsg) {
	cpSender := l.getNewCheckpointSender(paxosMsg.GetNodeID())

	if cpSender != nil {
		cpSender.start()
		lPLGHead(l.conf.groupIdx, "new checkpoint sender started, send to nodeid %d", paxosMsg.GetNodeID())
	} else {
		lPLGErr(l.conf.groupIdx, "Checkpoint Sender is running")
	}
}

func (l *learner) sendCheckpointBegin(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64) error {
	checkpointMsg := &paxospb.CheckpointMsg{}
	checkpointMsg.MsgType = checkpointMsgType_SendFile
	checkpointMsg.NodeID = l.conf.getMyNodeID()
	checkpointMsg.Flag = checkpointSendFileFlag_BEGIN
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID

	lPLGImp(l.conf.groupIdx, "END, SendNodeID %d uuid %d sequence %d cpi %d",
		sendNodeId, uuid, sequence, checkpointInstanceID)

	return l.sendCheckpointMessage(sendNodeId, checkpointMsg, TCP)
}

func (l *learner) sendCheckpointEnd(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64) error {
	checkpointMsg := &paxospb.CheckpointMsg{}
	checkpointMsg.MsgType = checkpointMsgType_SendFile
	checkpointMsg.NodeID = l.conf.getMyNodeID()
	checkpointMsg.Flag = checkpointSendFileFlag_END
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID

	lPLGImp(l.conf.groupIdx, "END, SendNodeID %d uuid %d sequence %d cpi %d", sendNodeId, uuid, sequence, checkpointInstanceID)

	return l.sendCheckpointMessage(sendNodeId, checkpointMsg, TCP)
}

func (l *learner) sendCheckpoint(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64, checksum uint32, filepath string, smID uint64, offset uint64, buf []byte) error {
	checkpointMsg := &paxospb.CheckpointMsg{}
	checkpointMsg.MsgType = checkpointMsgType_SendFile
	checkpointMsg.NodeID = l.conf.getMyNodeID()
	checkpointMsg.Flag = checkpointSendFileFlag_ING
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.CheckpointInstanceID = checkpointInstanceID
	checkpointMsg.Checksum = checksum
	checkpointMsg.FilePath = filepath
	checkpointMsg.SMID = smID
	checkpointMsg.Offset = offset
	checkpointMsg.Buffer = buf

	lPLGImp(l.conf.groupIdx, "END, SendNodeID %d uuid %d sequence %d cpi %d checksum %d smid %d offset %d buffsize %d filepath %s",
		sendNodeId, uuid, sequence, checkpointInstanceID,
		checksum, smID, offset, len(buf), filepath)

	return l.sendCheckpointMessage(sendNodeId, checkpointMsg, TCP)
}

func (l *learner) onSendCheckpointBegin(checkpointMsg *paxospb.CheckpointMsg) error {
	if err := l.cpReceiver.newReceiver(checkpointMsg.GetNodeID(), checkpointMsg.GetUUID()); err != nil {
		return err
	}

	lPLGImp(l.conf.groupIdx, "NewReceiver ok")

	if err := l.cpMgr.setMinChosenInstanceID(checkpointMsg.GetCheckpointInstanceID()); err != nil {
		lPLGErr(l.conf.groupIdx, "SetMinChosenInstanceID fail, CheckpointInstanceID %d, error: %v",
			checkpointMsg.GetCheckpointInstanceID(), err)
		return err
	}

	return nil
}

func (l *learner) onSendCheckpointIng(checkpointMsg *paxospb.CheckpointMsg) error {
	getBPInstance().OnSendCheckpointOneBlock()
	return l.cpReceiver.receiveCheckpoint(checkpointMsg)
}

func (l *learner) onSendCheckpointEnd(checkpointMsg *paxospb.CheckpointMsg) error {
	if !l.cpReceiver.isReceiverFinish(checkpointMsg.GetNodeID(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence()) {
		lPLGErr(l.conf.groupIdx, "receive end msg but receiver not finish")
		return errReceiverNotFinish
	}

	getBPInstance().ReceiveCheckpointDone()

	smList := l.smFac.getSMList()

	for _, sm := range smList {
		if sm.SMID() == system_v_smid || sm.SMID() == master_v_smid {
			//system variables sm no checkpoint
			//master variables sm no checkpoint
			continue
		}

		tmpDirPath := l.cpReceiver.getTmpDirPath(sm.SMID())
		filePathList, err := iterDir(tmpDirPath)
		if err != nil {
			lPLGErr(l.conf.groupIdx, "IterDir fail, dirpath %s", tmpDirPath)
		}

		if len(filePathList) == 0 {
			lPLGImp(l.conf.groupIdx, "this sm %d have no checkpoint", sm.SMID())
			continue
		}

		err = sm.LoadCheckpointState(l.conf.groupIdx, tmpDirPath, filePathList, checkpointMsg.GetCheckpointInstanceID())
		if err != nil {
			getBPInstance().ReceiveCheckpointAndLoadFail()
			return err
		}
	}

	getBPInstance().ReceiveCheckpointAndLoadSucc()
	lPLGImp(l.conf.groupIdx, "All sm load state ok, start to exit process")
	os.Exit(-1)

	return nil
}

func (l *learner) onSendCheckpoint(checkpointMsg *paxospb.CheckpointMsg) {
	lPLGHead(l.conf.groupIdx, "START uuid %d flag %d sequence %d cpi %d checksum %d smid %d offset %d buffsize %d filepath %s",
		checkpointMsg.GetUUID(), checkpointMsg.GetFlag(), checkpointMsg.GetSequence(),
		checkpointMsg.GetCheckpointInstanceID(), checkpointMsg.GetChecksum(), checkpointMsg.GetSMID(),
		checkpointMsg.GetOffset(), len(checkpointMsg.GetBuffer()), checkpointMsg.GetFilePath())

	var err error
	switch checkpointMsg.GetFlag() {
	case checkpointSendFileFlag_BEGIN:
		err = l.onSendCheckpointBegin(checkpointMsg)
	case checkpointSendFileFlag_ING:
		err = l.onSendCheckpointIng(checkpointMsg)
	case checkpointSendFileFlag_END:
		err = l.onSendCheckpointEnd(checkpointMsg)
	default:
		lPLGErr(l.conf.groupIdx, "unknown checkpoint flag")
	}

	if err != nil {
		lPLGErr(l.conf.groupIdx, "[FAIL] reset checkpoint receiver and reset askforlearn")

		l.cpReceiver.reset()
		l.resetAskForLearnNoop(time.Millisecond * 5000)
		l.sendCheckpointAck(checkpointMsg.GetNodeID(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence(), checkpointSendFileAckFlag_Fail)
	} else {
		l.sendCheckpointAck(checkpointMsg.GetNodeID(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence(), checkpointSendFileAckFlag_OK)
		l.resetAskForLearnNoop(time.Millisecond * 120000)
	}
}

func (l *learner) sendCheckpointAck(sendNodeId nodeId, uuid uint64, sequence uint64, flag int) error {
	checkpointMsg := &paxospb.CheckpointMsg{}
	checkpointMsg.MsgType = checkpointMsgType_SendFile_Ack
	checkpointMsg.NodeID = l.conf.getMyNodeID()
	checkpointMsg.UUID = uuid
	checkpointMsg.Sequence = sequence
	checkpointMsg.Flag = flag

	return l.sendCheckpointMessage(sendNodeId, checkpointMsg, TCP)
}

func (l *learner) onSendCheckpointAck(checkpointMsg *paxospb.CheckpointMsg) {
	lPLGHead(l.conf.groupIdx, "START flag %d", checkpointMsg.GetFlag())

	if l.cpSender != nil && !l.cpSender.hasBeenEnded() {
		if checkpointMsg.GetFlag() == checkpointSendFileAckFlag_OK {
			l.cpSender.ack(checkpointMsg.GetNodeID(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence())
		} else {
			l.cpSender.end()
		}
	}
}

func (l *learner) getNewCheckpointSender(sendNodeID nodeId) *checkpointSender {
	if l.cpSender != nil {
		if l.cpSender.hasBeenEnded() {
			l.cpSender = nil
		}
	}

	if l.cpSender == nil {
		l.cpSender = newCheckpointSender(sendNodeID, l.conf, l, l.smFac, l.cpMgr)
		return l.cpSender
	}

	return nil
}

func (l *learner) stop() {
	l.learnerSender.stop()
	if l.cpSender != nil {
		l.cpSender.stop()
	}

}

///////////////////////////////////////////////////////////////////////////////

type learnerState struct {
	learnedValue []byte
	isLearned    bool
	newChecksum  uint32
	conf         *config
	paxosLog     *paxosLog
}

func newLearnerState(conf *config, ls LogStorage) *learnerState {
	ret := &learnerState{}
	ret.conf = conf
	ret.paxosLog = newPaxosLog(ls)
	ret.init()

	return ret
}

func (l *learnerState) init() {
	l.learnedValue = []byte{}
	l.isLearned = false
	l.newChecksum = 0
}

func (l *learnerState) getNewChecksum() uint32 {
	return l.newChecksum
}

func (l *learnerState) learnValueWithoutWrite(instanceID uint64, value []byte, newChecksum uint32) {
	l.learnedValue = value
	l.isLearned = true
	l.newChecksum = newChecksum
}

func (l *learnerState) learnValue(instanceID uint64, learnedBallot *ballotNumber, value []byte, lastChecksum uint32) error {
	if instanceID > 0 && lastChecksum == 0 {
		l.newChecksum = 0
	} else if len(value) > 0 {
		l.newChecksum = crc32.ChecksumIEEE(value)
	}

	state := &paxospb.AcceptorStateData{}
	state.InstanceID = instanceID
	state.AcceptedValue = value
	state.PromiseID = learnedBallot.proposalID
	state.PromiseNodeID = learnedBallot.nodeID
	state.AcceptedID = learnedBallot.proposalID
	state.AcceptedNodeID = learnedBallot.nodeID
	state.Checksum = l.newChecksum

	wo := writeOptions(false)

	if err := l.paxosLog.writeState(wo, l.conf.getMyGroupIdx(), instanceID, state); err != nil {
		lPLGErr(l.conf.groupIdx, "LogStorage.WriteLog fail, InstanceID %d ValueLen %d, error: %v",
			instanceID, len(value), err)
		return err
	}

	l.learnValueWithoutWrite(instanceID, value, l.newChecksum)

	lPLGDebug(l.conf.groupIdx, "OK, InstanceID %d ValueLen %d checksum %d",
		instanceID, len(value), l.newChecksum)

	return nil
}

func (l *learnerState) getLearnValue() []byte {
	return l.learnedValue
}

func (l *learnerState) getIsLearned() bool {
	return l.isLearned
}
