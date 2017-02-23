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
