package gopaxos

import "github.com/buptmiao/gopaxos/paxospb"

type ballotNumber struct {
	proposalID uint64
	nodeID     nodeId
}

func newBallotNumber(pid uint64, nid nodeId) *ballotNumber {
	return &ballotNumber{
		proposalID: pid,
		nodeID:     nid,
	}
}

func unpackBaseMsg(buf []byte) (*paxospb.Header, int, int, error) {

}

///////////////////////////////////////////////////////////////////////////////
type msgCounter struct {
	conf                        *config
	receiveMsgNodeIDSet         map[nodeId]struct{}
	rejectMsgNodeIDSet          map[nodeId]struct{}
	promiseOrAcceptMsgNodeIDSet map[nodeId]struct{}
}

func newMsgCounter(conf *config) *msgCounter {
	ret := &msgCounter{
		conf: conf,
	}

	ret.startNewRound()
	return ret
}

func (m *msgCounter) startNewRound() {
	m.receiveMsgNodeIDSet = make(map[nodeId]struct{})
	m.rejectMsgNodeIDSet = make(map[nodeId]struct{})
	m.promiseOrAcceptMsgNodeIDSet = make(map[nodeId]struct{})
}

func (m *msgCounter) addReceive(id nodeId) {
	if _, ok := m.receiveMsgNodeIDSet[id]; !ok {
		m.receiveMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addReject(id nodeId) {
	if _, ok := m.rejectMsgNodeIDSet[id]; !ok {
		m.rejectMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addPromiseOrAccept(id nodeId) {
	if _, ok := m.promiseOrAcceptMsgNodeIDSet[id]; !ok {
		m.promiseOrAcceptMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) isPassedOnThisRound() bool {
	return len(m.promiseOrAcceptMsgNodeIDSet) >= m.conf.getMajorityCount()
}

func (m *msgCounter) isRejectedOnThisRound() bool {
	return len(m.rejectMsgNodeIDSet) >= m.conf.getMajorityCount()
}

func (m *msgCounter) isAllReceiveOnThisRound() bool {
	return len(m.receiveMsgNodeIDSet) >= m.conf.getMajorityCount()
}
