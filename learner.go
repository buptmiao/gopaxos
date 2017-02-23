package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

// learner implements the interface base.
type learner struct {
	conf       *config
	comm       MsgTransport
	instance   *instance
	instanceID uint64
	isTestMode bool

	state                      learnerState
	acceptor                   *acceptor
	paxosLog                   *paxosLog
	askForLearnNoopTimerID     uint32
	loop                       *ioLoop
	highestSeenInstanceID      uint64
	highestSeenInstanceIDOwner nodeId

	isIMLearning      bool
	learnerSender     learnerSender
	lastAckInstanceID uint64
	cpMgr             *checkpointMgr
	smFac             *smFac

	cpSender   *checkpointSender
	cpReceiver *checkpointReceiver
}

func newLearner(conf *config, tran MsgTransport, i *instance, acceptor *acceptor, ls LogStorage,
	loop *ioLoop, cpMgr *checkpointMgr, smFac *smFac) *learner {

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
}
