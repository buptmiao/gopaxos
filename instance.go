package gopaxos

import (
	"encoding/binary"
	"github.com/buptmiao/gopaxos/paxospb"
)

type instance struct {
	conf     *config
	comm     MsgTransport
	smFac    *smFac
	loop     *ioLoop
	acceptor *acceptor
	learner  *learner
	proposer *proposer
	paxosLog *paxosLog

	lastChecksum  uint32
	commitCtx     *commitCtx
	commitTimerID uint32
	committer     *committer

	checkpointMgr *checkpointMgr

	timeStat timeStat
	opt      *Options
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
	//Must init acceptor first, because the max instanceid is record in acceptor state.
	var err error
	if err = i.acceptor.init(); err != nil {
		lPLGErr("Acceptor.Init fail, error: %v", err)
		return err
	}

	if err = i.checkpointMgr.init(); err != nil {
		lPLGErr("CheckpointMgr.Init fail, error: %v", err)
		return err
	}

	cpInstanceID := i.checkpointMgr.getCheckpointInstanceID() + 1
	lPLGImp("Acceptor.OK, Log.InstanceID %d Checkpoint.InstanceID %d",
		i.acceptor.getInstanceID(), cpInstanceID)

	nowInstanceID := cpInstanceID
	if nowInstanceID < i.acceptor.getInstanceID() {
		if err = i.playLog(nowInstanceID, i.acceptor.getInstanceID()); err != nil {
			return err
		}

		lPLGImp("PlayLog OK, begin instanceid %d end instanceid %d", nowInstanceID, i.acceptor.getInstanceID())

		nowInstanceID = i.acceptor.getInstanceID()
	} else {
		if nowInstanceID > i.acceptor.getInstanceID() {
			if !i.protectionLogicIsCheckpointInstanceIDCorrect(nowInstanceID, i.acceptor.getInstanceID()) {
				return errCheckpointInstanceID
			}

			i.acceptor.initForNewPaxosInstance()
		}

		i.acceptor.setInstanceID(nowInstanceID)
	}

	lPLGImp(i.conf.groupIdx, "NowInstanceID %d", nowInstanceID)

	i.learner.setInstanceID(nowInstanceID)
	i.proposer.setInstanceID(nowInstanceID)
	i.proposer.setStartProposalID(i.acceptor.getAcceptorState().getPromiseBallot().proposalID + 1)

	i.checkpointMgr.setMaxChosenInstanceID(nowInstanceID)

	if err = i.initLastChecksum(); err != nil {
		return err
	}

	i.learner.resetAskForLearnNoop(getInsideOptionsInstance().getAskforLearnInterval())

	lPLGImp(i.conf.groupIdx, "OK")

	return nil
}

func (i *instance) start() {
	i.learner.startLearnerSender()
	i.loop.start()
	//start checkpoint replayer and cleaner
	i.checkpointMgr.start()
}

func (i *instance) protectionLogicIsCheckpointInstanceIDCorrect(cpInstanceID, logMaxInstanceID uint64) bool {
	if cpInstanceID <= logMaxInstanceID+1 {
		return true
	}
	//checkpoint_instanceid larger than log_maxinstanceid+1 will appear in the following situations
	//1. Pull checkpoint from other node automatically and restart. (normal case)
	//2. Paxos log was manually all deleted. (may be normal case)
	//3. Paxos log is lost because Options::bSync set as false. (bad case)
	//4. Checkpoint data corruption results an error checkpoint_instanceid. (bad case)
	//5. Checkpoint data copy from other node manually. (bad case)
	//In these bad cases, paxos log between [log_maxinstanceid, checkpoint_instanceid) will not exist
	//and checkpoint data maybe wrong, we can't ensure consistency in this case.
	if logMaxInstanceID == 0 {
		//case 1. Automatically pull checkpoint will delete all paxos log first.
		//case 2. No paxos log.
		//If minchosen instanceid < checkpoint instanceid.
		//Then Fix minchosen instanceid to avoid that paxos log between [log_maxinstanceid, checkpoint_instanceid) not exist.
		//if minchosen isntanceid > checkpoint.instanceid.
		//That probably because the automatic pull checkpoint did not complete successfully.
		minChosenInstanceID := i.checkpointMgr.getMinChosenInstanceID()
		if i.checkpointMgr.getMinChosenInstanceID() != cpInstanceID {
			if err := i.checkpointMgr.setMinChosenInstanceID(cpInstanceID); err != nil {
				lPLGErr("SetMinChosenInstanceID fail, now minchosen %d max instanceid %d checkpoint instanceid %d",
					i.checkpointMgr.getMinChosenInstanceID(), logMaxInstanceID, cpInstanceID)
				return false
			}

			lPLGStatus("Fix minchonse instanceid ok, old minchosen %d now minchosen %d max %d checkpoint %d",
				minChosenInstanceID, i.checkpointMgr.getMinChosenInstanceID(), logMaxInstanceID, cpInstanceID)
		}

		return true
	} else {
		//other case.
		lPLGErr(`checkpoint instanceid %d larger than log max instanceid %d. Please ensure that your checkpoint data is correct. If you ensure that, just delete all paxos log data and restart.`,
			cpInstanceID, logMaxInstanceID)
		return false
	}
}

func (i *instance) initLastChecksum() error {
	if i.acceptor.getInstanceID() == 0 {
		i.lastChecksum = 0
		return nil
	}

	if i.acceptor.getInstanceID() <= i.checkpointMgr.getMinChosenInstanceID() {
		i.lastChecksum = 0
		return nil
	}

	state, err := i.paxosLog.readState(i.conf.getMyGroupIdx(), i.acceptor.getInstanceID()-1)
	if err != nil && err != ErrNotFoundFromStorage {
		return err
	}

	if err == ErrNotFoundFromStorage {
		lPLGErr(i.conf.groupIdx, "las checksum not exist, now instanceid %d", i.acceptor.getInstanceID())
		i.lastChecksum = 0
		return nil
	}

	i.lastChecksum = state.GetChecksum()

	lPLGImp(i.conf.groupIdx, "ok, last checksum %d", i.lastChecksum)

	return nil
}

func (i *instance) playLog(beginInstanceID, endInstanceID uint64) error {
	if beginInstanceID < i.checkpointMgr.getMinChosenInstanceID() {
		lPLGErr(i.conf.groupIdx, "now instanceid %d small than min chosen instanceid %d",
			beginInstanceID, i.checkpointMgr.getMinChosenInstanceID())
		return errSmallThanMinChosenInstanceID
	}

	for instanceID := beginInstanceID; instanceID < endInstanceID; instanceID++ {
		state, err := i.paxosLog.readState(i.conf.getMyGroupIdx(), instanceID)
		if err != nil {
			lPLGErr(i.conf.groupIdx, "log read fail, instanceid %d, error: %v", instanceID, err)
			return err
		}

		if !i.smFac.execute(i.conf.getMyGroupIdx(), instanceID, state.GetAcceptedValue(), nil) {
			lPLGErr(i.conf.groupIdx, "Execute fail, instanceid %d", instanceID)
			return errInstanceExecuteFailed
		}
	}

	return nil
}

func (i *instance) getLastChecksum() uint32 {
	return i.lastChecksum
}

func (i *instance) getCommitter() *committer {
	return i.committer
}

func (i *instance) getCheckpointCleaner() *cleaner {
	return i.checkpointMgr.getCleaner()
}

func (i *instance) getCheckpointRePlayer() *rePlayer {
	return i.checkpointMgr.getRePlayer()
}

func (i *instance) checkNewValue() {
	if !i.commitCtx.isNewCommit() {
		return
	}

	if !i.learner.isIMLatest() {
		return
	}

	if i.conf.isIMFollower() {
		lPLGErr(i.conf.groupIdx, "I'm follower, skip this new value")
		i.commitCtx.setResultOnlyRet(paxostrycommitret_follower_cannot_commit)
		return
	}

	if !i.conf.checkConfig() {
		lPLGErr(i.conf.groupIdx, "I'm not in membership, skip this new value")
		i.commitCtx.setResultOnlyRet(paxostrycommitret_im_not_in_membership)
		return
	}

	if len(i.commitCtx.getCommitValue()) > getInsideOptionsInstance().getMaxBufferSize() {
		lPLGErr(i.conf.groupIdx, "value size %d to large, skip this new value",
			len(i.commitCtx.getCommitValue()))
		i.commitCtx.setResultOnlyRet(paxostrycommitret_value_size_toolarge)
		return
	}

	i.commitCtx.startCommit(i.proposer.getInstanceID())

	if i.commitCtx.getTimeoutMs() != -1 {
		i.commitTimerID, _ = i.loop.addTimer(i.commitCtx.getTimeoutMs(), timer_Instance_Commit_Timeout)
	}

	i.timeStat.point()

	if i.conf.getIsUseMembership() && (i.proposer.getInstanceID() == 0 || i.conf.getGid() == 0) {
		lPLGHead(i.conf.groupIdx, "Need to init system variables, Now.InstanceID %d Now.Gid %d",
			i.proposer.getInstanceID(), i.conf.getGid())

		gid := getGid(i.conf.getMyNodeID())
		value, err := i.conf.getSystemVSM().createGidOPValue(gid)
		if err != nil {
			lPLGErr(i.conf.groupIdx, "create gid op value failed, error: %v", err)
			return
		}

		i.smFac.packPaxosValue(value, i.conf.getSystemVSM().SMID())
		i.proposer.newValue(value)
	} else {
		if i.opt.IsOpenChangeValueBeforePropose {
			v := i.smFac.beforePropose(i.conf.groupIdx, i.commitCtx.getCommitValue())
			i.commitCtx.setCommitValue(v)
		}

		i.proposer.newValue(i.commitCtx.getCommitValue())
	}
}

func (i *instance) onNewValueCommitTimeout() {
	getBPInstance().OnNewValueCommitTimeout()

	i.proposer.exitPrepare()
	i.proposer.exitAccept()

	i.commitCtx.setResult(paxostrycommitret_timeout, i.proposer.getInstanceID(), "")
}

func (i *instance) onReceiveMessage(msg []byte) error {
	i.loop.addMessage(msg)

	return nil
}

func (i *instance) receiveMsgHeaderCheck(header *paxospb.Header, fromNodeID nodeId) bool {
	if i.conf.getGid() == 0 || header.GetGid() == 0 {
		return true
	}

	if i.conf.getGid() != header.GetGid() {
		getBPInstance().HeaderGidNotSame()

		lPLGErr(i.conf.groupIdx, "Header check fail, header.gid %d config.gid %d, msg.from_nodeid %d",
			header.GetGid(), i.conf.getGid(), fromNodeID)

		return false
	}

	return true
}

func (i *instance) onReceive(buf []byte) {
	getBPInstance().OnReceive()

	if len(buf) <= 6 {
		lPLGErr(i.conf.groupIdx, "buffer size %d too short", len(buf))
		return
	}

	header, bodyStartPos, bodyLen, err := unpackBaseMsg(buf)
	if err != nil {
		return
	}

	if header.GetCmdid() == msgCmd_PaxosMsg {
		if i.checkpointMgr.inAskForCheckpointMode() {
			lPLGImp(i.conf.groupIdx, "in ask for checkpoint mode, ignord paxosmsg")
			return
		}

		paxosMsg := &paxospb.PaxosMsg{}
		if err := paxosMsg.Unmarshal(buf[bodyStartPos : bodyStartPos+bodyLen]); err != nil {
			getBPInstance().OnReceiveParseError()
			lPLGErr(i.conf.groupIdx, "PaxosMsg.Unmarshal fail, skip this msg")
			return
		}

		if !i.receiveMsgHeaderCheck(header, paxosMsg.GetNodeID()) {
			return
		}

		i.onReceivePaxosMsg(paxosMsg, false)
	} else if header.GetCmdid() == msgCmd_CheckpointMsg {
		checkpointMsg := &paxospb.CheckpointMsg{}
		if err := checkpointMsg.Unmarshal(buf[bodyStartPos : bodyStartPos+bodyLen]); err != nil {
			getBPInstance().OnReceiveParseError()
			lPLGErr(i.conf.groupIdx, "PaxosMsg.Unmarshal fail, skip this msg")
			return
		}

		if !i.receiveMsgHeaderCheck(header, checkpointMsg.GetNodeID()) {
			return
		}

		i.onReceiveCheckpointMsg(checkpointMsg)
	}
}

func (i *instance) onReceiveCheckpointMsg(checkpointMsg *paxospb.CheckpointMsg) {
	lPLGImp(i.conf.groupIdx, `Now.InstanceID %d MsgType %d Msg.from_nodeid %d My.nodeid %d flag %d
	 	uuid %d sequence %d checksum %d offset %d buffsize %d filepath %s`,
		i.acceptor.getInstanceID(), checkpointMsg.GetMsgType(), checkpointMsg.GetNodeID(),
		i.conf.getMyNodeID(), checkpointMsg.GetFlag(), checkpointMsg.GetUUID(), checkpointMsg.GetSequence(), checkpointMsg.GetChecksum(),
		checkpointMsg.GetOffset(), len(checkpointMsg.GetBuffer()), len(checkpointMsg.GetFilePath()))

	if checkpointMsg.GetMsgType() == checkpointMsgType_SendFile {
		if !i.checkpointMgr.inAskForCheckpointMode() {
			lPLGImp(i.conf.groupIdx, "not in ask for checkpoint mode, ignord checkpoint msg")
			return
		}

		i.learner.onSendCheckpoint(checkpointMsg)
	} else if checkpointMsg.GetMsgType() == checkpointMsgType_SendFile_Ack {
		i.learner.onSendCheckpointAck(checkpointMsg)
	}
}

func (i *instance) onReceivePaxosMsg(paxosMsg *paxospb.PaxosMsg, isRetry bool) error {
	getBPInstance().OnReceivePaxosMsg()

	lPLGImp(i.conf.groupIdx, "Now.InstanceID %d Msg.InstanceID %d MsgType %d Msg.from_nodeid %d My.nodeid %d Seen.LatestInstanceID %d",
		i.proposer.getInstanceID(), paxosMsg.GetInstanceID(), paxosMsg.GetMsgType(),
		paxosMsg.GetNodeID(), i.conf.getMyNodeID(), i.learner.getSeenLatestInstanceID())

	switch paxosMsg.GetMsgType() {

	case msgType_PaxosPrepareReply, msgType_PaxosAcceptReply, msgType_PaxosProposal_SendNewValue:
		if !i.conf.isValidNodeID(paxosMsg.GetNodeID()) {
			getBPInstance().OnReceivePaxosMsgNodeIDNotValid()
			lPLGErr(i.conf.groupIdx, "acceptor reply type msg, from nodeid not in my membership, skip this message")
			return nil
		}

		return i.receiveMsgForProposer(paxosMsg)

	case msgType_PaxosPrepare, msgType_PaxosAccept:
		if i.conf.getGid() == 0 {
			i.conf.addTmpNodeOnlyForLearn(paxosMsg.GetNodeID())
		}

		if !i.conf.isValidNodeID(paxosMsg.GetNodeID()) {
			lPLGErr(i.conf.groupIdx, `prepare/accept type msg, from nodeid not in my membership(or i'm null membership),
			skip this message and add node to tempnode, my gid %d`,
				i.conf.getGid())

			i.conf.addTmpNodeOnlyForLearn(paxosMsg.GetNodeID())

			return nil
		}

		i.checksumLogic(paxosMsg)

		return i.receiveMsgForAcceptor(paxosMsg, isRetry)

	case msgType_PaxosLearner_AskforLearn, msgType_PaxosLearner_SendLearnValue, msgType_PaxosLearner_ProposerSendSuccess,
		msgType_PaxosLearner_ConfirmAskforLearn, msgType_PaxosLearner_SendNowInstanceID, msgType_PaxosLearner_SendLearnValue_Ack,
		msgType_PaxosLearner_AskforCheckpoint:

		i.checksumLogic(paxosMsg)
		return i.receiveMsgForLearner(paxosMsg)

	default:
		getBPInstance().OnReceivePaxosMsgTypeNotValid()
		lPLGErr(i.conf.groupIdx, "Invaid msgtype %d", paxosMsg.GetMsgType())
	}

	return nil
}

func (i *instance) receiveMsgForProposer(paxosMsg *paxospb.PaxosMsg) error {
	if i.conf.isIMFollower() {
		lPLGErr(i.conf.groupIdx, "I'm follower, skip this message")
		return nil
	}

	if paxosMsg.GetInstanceID() != i.proposer.getInstanceID() {
		if paxosMsg.GetInstanceID()+1 == i.proposer.getInstanceID() {
			//Expired reply msg on last instance.
			//If the response of a node is always slower than the majority node,
			//then the message of the node is always ignored even if it is a reject reply.
			//In this case, if we do not deal with these reject reply, the node that
			//gave reject reply will always give reject reply.
			//This causes the node to remain in catch-up state.
			//
			//To avoid this problem, we need to deal with the expired reply.
			if paxosMsg.GetMsgType() == msgType_PaxosPrepareReply {
				i.proposer.onExpirePrepareReply(paxosMsg)
			} else if paxosMsg.GetMsgType() == msgType_PaxosAcceptReply {
				i.proposer.onExpiredAcceptReply(paxosMsg)
			}
		}
		getBPInstance().OnReceivePaxosProposerMsgINotSame()

		return nil
	}

	if paxosMsg.GetMsgType() == msgType_PaxosPrepareReply {
		i.proposer.onPrepareReply(paxosMsg)
	} else if paxosMsg.GetMsgType() == msgType_PaxosAcceptReply {
		i.proposer.onAcceptReply(paxosMsg)
	}

	return nil
}

func (i *instance) receiveMsgForAcceptor(paxosMsg *paxospb.PaxosMsg, isRetry bool) error {
	if i.conf.isIMFollower() {
		lPLGErr(i.conf.groupIdx, "I'm follower, skip this message")
		return nil
	}

	if paxosMsg.GetInstanceID() != i.acceptor.getInstanceID() {
		getBPInstance().OnReceivePaxosAcceptorMsgINotSame()
	}

	if paxosMsg.GetInstanceID() == i.acceptor.getInstanceID()+1 {
		//skip success message
		newPaxosMsg := &paxospb.PaxosMsg{}
		newPaxosMsg.InstanceID = i.acceptor.getInstanceID()
		newPaxosMsg.MsgType = msgType_PaxosLearner_ProposerSendSuccess

		i.receiveMsgForLearner(newPaxosMsg)
	}

	if paxosMsg.GetInstanceID() == i.acceptor.getInstanceID() {
		if paxosMsg.GetMsgType() == msgType_PaxosPrepare {
			return i.acceptor.onPrepare(paxosMsg)
		} else if paxosMsg.GetMsgType() == msgType_PaxosAccept {
			i.acceptor.onAccept(paxosMsg)
		}
	} else if !isRetry && paxosMsg.GetInstanceID() > i.acceptor.getInstanceID() {
		if paxosMsg.GetInstanceID() >= i.learner.getSeenLatestInstanceID() {
			if paxosMsg.GetInstanceID() < i.acceptor.getInstanceID()+retryQueueMaxLen {
				//need retry msg precondition
				//1. prepare or accept msg
				//2. msg.instanceid > nowinstanceid.
				//    (if < nowinstanceid, this msg is expire)
				//3. msg.instanceid >= seen latestinstanceid.
				//    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
				//4. msg.instanceid close to nowinstanceid.
				i.loop.addRetryPaxosMsg(paxosMsg)
				getBPInstance().OnReceivePaxosAcceptorMsgAddRetry()
			} else {
				i.loop.clearRetryQueue()
			}
		}
	}

	return nil
}

func (i *instance) receiveMsgForLearner(paxosMsg *paxospb.PaxosMsg) error {

	switch paxosMsg.GetMsgType() {
	case msgType_PaxosLearner_AskforLearn:
		i.learner.onAskForLearn(paxosMsg)
	case msgType_PaxosLearner_SendLearnValue:
		i.learner.onSendLearnValue(paxosMsg)
	case msgType_PaxosLearner_ProposerSendSuccess:
		i.learner.onProposerSendSuccess(paxosMsg)
	case msgType_PaxosLearner_SendNowInstanceID:
		i.learner.onSendNowInstanceID(paxosMsg)
	case msgType_PaxosLearner_ConfirmAskforLearn:
		i.learner.onConfirmAskForLearn(paxosMsg)
	case msgType_PaxosLearner_SendLearnValue_Ack:
		i.learner.onSendLearnValueAck(paxosMsg)
	case msgType_PaxosLearner_AskforCheckpoint:
		i.learner.onAskForCheckpoint(paxosMsg)
	}

	if i.learner.isLearned() {
		getBPInstance().OnInstanceLearned()

		ctx, isMyCommit := i.commitCtx.isMyCommit(i.learner.getInstanceID(), i.learner.getLearnValue())

		if !isMyCommit {
			getBPInstance().OnInstanceLearnedNotMyCommit()
			lPLGDebug(i.conf.groupIdx, "this value is not my commit")
		} else {
			useTimeMs := i.timeStat.point()
			getBPInstance().OnInstanceLearnedIsMyCommit(useTimeMs)
			lPLGHead(i.conf.groupIdx, "My commit ok, usetime %dms", useTimeMs)
		}

		if !i.smExecute(i.learner.getInstanceID(), i.learner.getLearnValue(), isMyCommit, ctx) {
			getBPInstance().OnInstanceLearnedSMExecuteFail()

			lPLGErr(i.conf.groupIdx, "SMExecute fail, instanceid %d, not increase instanceid", i.learner.getInstanceID())
			i.commitCtx.setResult(paxostrycommitret_executefail, i.learner.getInstanceID(), i.learner.getLearnValue())

			i.proposer.cancelSkipPrepare()

			return errInstanceExecuteFailed
		}
		//this paxos instance end, tell proposal done
		i.commitCtx.setResult(paxostrycommitret_ok, i.learner.getInstanceID(), i.learner.getLearnValue())

		if i.commitTimerID > 0 {
			i.loop.removeTimer(i.commitTimerID)
		}

		lPLGHead(i.conf.groupIdx, "[Learned] New paxos starting, Now.Proposer.InstanceID %d"+
			"Now.Acceptor.InstanceID %d Now.Learner.InstanceID %d",
			i.proposer.getInstanceID(), i.acceptor.getInstanceID(), i.learner.getInstanceID())

		lPLGHead(i.conf.groupIdx, "[Learned] Checksum change, last checksum %d new checksum %d",
			i.lastChecksum, i.learner.getNewChecksum())

		i.lastChecksum = i.learner.getNewChecksum()

		i.newInstance()

		lPLGHead(i.conf.groupIdx, "[Learned] New paxos instance has started, Now.Proposer.InstanceID %d "+
			"Now.Acceptor.InstanceID %d Now.Learner.InstanceID %d",
			i.proposer.getInstanceID(), i.acceptor.getInstanceID(), i.learner.getInstanceID())

		i.checkpointMgr.setMaxChosenInstanceID(i.acceptor.getInstanceID())

		getBPInstance().NewInstance()
	}

	return nil
}

func (i *instance) newInstance() {
	i.acceptor.newInstance()
	i.learner.newInstance()
	i.proposer.newInstance()
}

func (i *instance) getNowInstanceID() uint64 {
	return i.acceptor.getInstanceID()
}

func (i *instance) onTimeout(timerID uint32, typ int) {
	switch typ {
	case timer_Proposer_Prepare_Timeout:
		i.proposer.onPrepareTimeout()
	case timer_Proposer_Accept_Timeout:
		i.proposer.onAcceptTimeout()
	case timer_Learner_Askforlearn_noop:
		i.learner.askForLearnNoop()
	case timer_Instance_Commit_Timeout:
		i.onNewValueCommitTimeout()
	default:
		lPLGErr(i.conf.groupIdx, "unknown timer type %d, timerid %d", typ, timerID)
	}
}

func (i *instance) addStateMachine(sm StateMachine) {
	i.smFac.addSM(sm)
}

func (i *instance) smExecute(instanceID uint64, value []byte, isMyCommit bool, ctx *SMCtx) bool {
	return i.smFac.execute(i.conf.getMyGroupIdx(), instanceID, value, ctx)
}

func (i *instance) checksumLogic(paxosMsg *paxospb.PaxosMsg) {
	if paxosMsg.GetLastChecksum() == 0 {
		return
	}

	if paxosMsg.GetInstanceID() != i.acceptor.getInstanceID() {
		return
	}

	if i.acceptor.getInstanceID() > 0 && i.getLastChecksum() == 0 {
		lPLGErr(i.conf.groupIdx, "I have no last checksum, other last checksum %d", paxosMsg.GetLastChecksum())
		i.lastChecksum = paxosMsg.GetLastChecksum()
		return
	}

	lPLGHead(i.conf.groupIdx, "my last checksum %d other last checksum %d", i.getLastChecksum(), paxosMsg.GetLastChecksum())

	if paxosMsg.GetLastChecksum() != i.getLastChecksum() {
		lPLGErr(i.conf.groupIdx, "checksum fail, my last checksum %d other last checksum %d",
			i.getLastChecksum(), paxosMsg.GetLastChecksum())
		getBPInstance().ChecksumLogicFail()
	}

	if paxosMsg.GetLastChecksum() != i.getLastChecksum() {
		lPLGErr(i.conf.groupIdx, "unexpected lastchecksum")
	}
}

func (i *instance) getInstanceValue(instanceID uint64) ([]byte, int64, error) {
	if instanceID >= i.acceptor.getInstanceID() {
		return nil, 0, errGetInstanceValueNotChosen
	}

	state, err := i.paxosLog.readState(i.conf.groupIdx, instanceID)
	if err != nil && err != ErrNotFoundFromStorage {
		return nil, 0, err
	}

	if err == ErrNotFoundFromStorage {
		return nil, 0, errGetInstanceValueNotExist
	}

	smID := int64(binary.LittleEndian.Uint64(state.GetAcceptedValue()))

	return smID, state.GetAcceptedValue()[8:], nil
}

func (i *instance) stop() {
	i.loop.stop()
	i.checkpointMgr.stop()
	i.learner.stop()
}
