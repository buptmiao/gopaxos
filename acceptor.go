package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

type acceptor struct {
}

func newAcceptor(conf *config, tran MsgTransport, i *instance, ls LogStorage) *acceptor {

}

func (a *acceptor) newInstance() {

}

func (a *acceptor) init() error {

}

func (a *acceptor) getInstanceID() uint64 {

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
}

func (a *acceptorState) getPromiseBallot() *ballotNumber {

}
