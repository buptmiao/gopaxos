package gopaxos

import (
	"hash/crc32"

	"github.com/buptmiao/gopaxos/paxospb"
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

	ret := &acceptor{
		base: newBase(conf, tran, i),
	}

}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////

func (l *learner) getInstanceID() uint64 {

}

func (l *learner) newInstance() {

}

func (l *learner) packMsg(paxosMsg *paxospb.PaxosMsg) ([]byte, error) {

}

func (l *learner) packCheckpointMsg(checkpointMsg *paxospb.CheckpointMsg) ([]byte, error) {

}

func (l *learner) getLastChecksum() uint32 {

}

func (l *learner) packBaseMsg(body []byte, cmd int) []byte {

}

func (l *learner) setAsTestMode() {

}

func (l *learner) sendPaxosMessage(sendToNodeID nodeId, paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (l *learner) broadcastMessage(paxosMsg *paxospb.PaxosMsg, runSelfFirst int, protocol TransportType) error {

}

func (l *learner) broadcastMessageToFollower(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (l *learner) broadcastMessageTempNode(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (l *learner) sendCheckpointMessage(sendToNodeID nodeId, checkpointMsg *paxospb.CheckpointMsg, protocol TransportType) error {

}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////

func (l *learner) startLearnerSender() {

}

func (l *learner) isLearned() bool {

}

func (l *learner) getLearnValue() []byte {

}

func (l *learner) initForNewPaxosInstance() {

}

func (l *learner) getNewChecksum() uint32 {

}

func (l *learner) isIMLatest() bool {

}

func (l *learner) setInstanceID(instanceID uint64) {

}

func (l *learner) getSeenLatestInstanceID() uint64 {

}

func (l *learner) setSeenInstanceID(instanceID uint64, fromNodeID nodeId) {

}

func (l *learner) resetAskForLearnNoop(timeout int) {

}

func (l *learner) askForLearnNoop(isStart bool) {

}

func (l *learner) askForLearn() {

}

func (l *learner) onAskForLearn(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) sendNowInstanceID(instanceID uint64, sendNodeID nodeId) {

}

func (l *learner) onSendNowInstanceID(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) confirmAskForLearn(sendNodeID nodeId) {

}

func (l *learner) onConfirmAskForLearn(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) sendLearnValue(sendNodeID nodeId, learnInstanceID uint64, learnedBallot *ballotNumber, learnedValue []byte, checksum uint32, needAck bool) error {

}

func (l *learner) onSendLearnValue(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) sendLearnValueAck(sendNodeID nodeId) {

}

func (l *learner) onSendLearnValueAck(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) transmitToFollower() {

}

func (l *learner) proposerSendSuccess(learnInstanceID uint64, proposalID uint64) {

}

func (l *learner) onProposerSendSuccess(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) askForCheckpoint(sendNodeID nodeId) {

}

func (l *learner) onAskForCheckpoint(paxosMsg *paxospb.PaxosMsg) {

}

func (l *learner) sendCheckpointBegin(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64) error {

}

func (l *learner) sendCheckpointEnd(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64) error {

}

func (l *learner) sendCheckpoint(sendNodeId nodeId, uuid uint64, sequence uint64, checkpointInstanceID uint64, checksum uint32, filepath string, smID uint64, offset uint64, buf []byte) error {

}

func (l *learner) onSendCheckpointBegin(checkpointMsg *paxospb.CheckpointMsg) error {

}

func (l *learner) onSendCheckpointIng(checkpointMsg *paxospb.CheckpointMsg) error {

}

func (l *learner) onSendCheckpointEnd(checkpointMsg *paxospb.CheckpointMsg) error {

}

func (l *learner) onSendCheckpoint(checkpointMsg *paxospb.CheckpointMsg) {

}

func (l *learner) sendCheckpointAck(checkpointMsg *paxospb.CheckpointMsg) {

}

func (l *learner) onSendCheckpointAck(checkpointMsg *paxospb.CheckpointMsg) {

}

func (l *learner) getNewCheckpointSender(sendNodeID nodeId) *checkpointSender {

}

func (l *learner) stop() {

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
