package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

type proposer struct {
	base
}

func newProposer(conf *config, tran MsgTransport, i *instance, learner *learner, loop *ioLoop) *proposer {

}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////

func (p *proposer) getInstanceID() uint64 {

}

func (p *proposer) newInstance() {

}

func (p *proposer) packMsg(paxosMsg *paxospb.PaxosMsg) ([]byte, error) {

}

func (p *proposer) packCheckpointMsg(checkpointMsg *paxospb.CheckpointMsg) ([]byte, error) {

}

func (p *proposer) getLastChecksum() uint32 {

}

func (p *proposer) packBaseMsg(body []byte, cmd int) []byte {

}

func (p *proposer) setAsTestMode() {

}

func (p *proposer) sendPaxosMessage(sendToNodeID nodeId, paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (p *proposer) broadcastMessage(paxosMsg *paxospb.PaxosMsg, runSelfFirst int, protocol TransportType) error {

}

func (p *proposer) broadcastMessageToFollower(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (p *proposer) broadcastMessageTempNode(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (p *proposer) sendCheckpointMessage(sendToNodeID nodeId, checkpointMsg *paxospb.CheckpointMsg, protocol TransportType) error {

}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////
func (p *proposer) isWorking() bool {

}

func (p *proposer) newValue(value []byte) {

}

func (p *proposer) exitPrepare() {

}

func (p *proposer) exitAccept() {

}

func (p *proposer) setInstanceID(instanceID uint64) {

}

func (p *proposer) setStartProposalID(id uint64) {

}

func (p *proposer) prepare(needNewBallot bool) {

}

func (p *proposer) onPrepareReply(paxosMsg *paxospb.PaxosMsg) {

}

func (p *proposer) onExpirePrepareReply(paxosMsg *paxospb.PaxosMsg) {

}

func (p *proposer) accept() {

}

func (p *proposer) onAcceptReply(paxosMsg *paxospb.PaxosMsg) {

}

func (p *proposer) onExpiredAcceptReply(paxosMsg *paxospb.PaxosMsg) {

}

func (p *proposer) onPrepareTimeout() {

}

func (p *proposer) onAcceptTimeout() {

}

func (p *proposer) cancelSkipPrepare() {

}
