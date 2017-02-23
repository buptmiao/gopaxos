package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

type proposer struct {
}

func newProposer(conf *config, tran MsgTransport, i *instance, learner *learner, loop *ioLoop) *proposer {

}

func (p *proposer) newInstance() {

}

func (p *proposer) isWorking() bool {

}

func (p *proposer) newValue(value []byte) {

}

func (p *proposer) exitPrepare() {

}

func (p *proposer) exitAccept() {

}

func (p *proposer) getInstanceID() uint64 {

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
