package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

type acceptor struct {
	base
	state *acceptorState
}

func newAcceptor(conf *config, tran MsgTransport, i *instance, ls LogStorage) *acceptor {
	ret := &acceptor{}

}

///////////////////////////////////////////////////////////////////////////////
// base methods
///////////////////////////////////////////////////////////////////////////////

func (a *acceptor) getInstanceID() uint64 {

}

func (a *acceptor) newInstance() {

}

func (a *acceptor) packMsg(paxosMsg *paxospb.PaxosMsg) ([]byte, error) {

}

func (a *acceptor) packCheckpointMsg(checkpointMsg *paxospb.CheckpointMsg) ([]byte, error) {

}

func (a *acceptor) getLastChecksum() uint32 {

}

func (a *acceptor) packBaseMsg(body []byte, cmd int) []byte {

}

func (a *acceptor) setAsTestMode() {

}

func (a *acceptor) sendPaxosMessage(sendToNodeID nodeId, paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (a *acceptor) broadcastMessage(paxosMsg *paxospb.PaxosMsg, runSelfFirst int, protocol TransportType) error {

}

func (a *acceptor) broadcastMessageToFollower(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (a *acceptor) broadcastMessageTempNode(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {

}

func (a *acceptor) sendCheckpointMessage(sendToNodeID nodeId, checkpointMsg *paxospb.CheckpointMsg, protocol TransportType) error {

}

///////////////////////////////////////////////////////////////////////////////
// respective methods
///////////////////////////////////////////////////////////////////////////////

func (a *acceptor) init() error {

}

func (a *acceptor) setInstanceID(instanceID uint64) {

}

func (a *acceptor) initForNewPaxosInstance() {

}

func (a *acceptor) getAcceptorState() *acceptorState {

}

func (a *acceptor) onPrepare(paxosMsg *paxospb.PaxosMsg) error {

}

func (a *acceptor) onAccept(paxosMsg *paxospb.PaxosMsg) {

}

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

}

func (a *acceptorState) init() {

}

func (a *acceptorState) getPromiseBallot() *ballotNumber {

}

func (a *acceptorState) getAcceptedBallot() *ballotNumber {

}

func (a *acceptorState) getAcceptedValue() []byte {

}

func (a *acceptorState) setAcceptedValue(value []byte) {

}

func (a *acceptorState) getChecksum() uint32 {

}

func (a *acceptorState) persist(instanceID uint64, lastChecksum uint32) error {

}

func (a *acceptorState) load() (uint64, error) {

}
