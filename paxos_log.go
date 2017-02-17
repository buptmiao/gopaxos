package paxos

import (
	"github.com/buptmiao/gopaxos/paxospb"
)

type paxosLog struct {
	logStorage LogStorage
}

func newPaxosLog(ls LogStorage) *paxosLog {
	return &paxosLog{
		logStorage: ls,
	}
}

func (p *paxosLog) writeLog(wo writeOptions, groupIdx int, instanceID uint64, value []byte) error {
	state := &paxospb.AcceptorStateData{}
	state.InstanceID = instanceID
	state.AcceptedValue = value
	state.PromiseID = 0
	state.PromiseNodeID = nullNode
	state.AcceptedID = 0
	state.AcceptedNodeID = nullNode

	if err := p.writeState(wo, groupIdx, instanceID, state); err != nil {
		lPLG1Err(groupIdx, "WriteState to db fail, groupidx %d instanceid %d err: %v", groupIdx, instanceID, err)
		return err
	}

	lPLG1Imp(groupIdx, "OK, groupidx %d InstanceID %d valuelen %d",
		groupIdx, instanceID, len(value))

	return nil
}

func (p *paxosLog) readLog(groupIdx int, instanceID uint64) ([]byte, error) {
	state, err := p.readState(groupIdx, instanceID)
	if err != nil {
		lPLG1Err(groupIdx, "ReadState from db fail, groupidx %d instanceid %d err: %v",
			groupIdx, instanceID, err)
		return nil, err
	}

	value := state.GetAcceptedValue()

	lPLG1Imp(groupIdx, "OK, groupidx %d InstanceID %d value %d",
		groupIdx, instanceID, len(value))

	return value, nil
}

func (p *paxosLog) writeState(wo writeOptions, groupIdx int, instanceID uint64, state *paxospb.AcceptorStateData) error {
	value, err := state.Marshal()
	if err != nil {
		lPLG1Err(groupIdx, "State.Marshal fail")
		return err
	}

	err = p.logStorage.Put(wo, groupIdx, instanceID, value)
	if err != nil {
		lPLG1Err(groupIdx, "DB.Put fail, groupidx %d bufferlen %d err: %v",
			groupIdx, len(value), err)
		return err
	}

	return nil
}

func (p *paxosLog) readState(groupIdx int, instanceID uint64) (*paxospb.AcceptorStateData, error) {
	value, err := p.logStorage.Get(groupIdx, instanceID)
	if err != nil && err != ErrNotFoundFromStorage {
		lPLG1Err(groupIdx, "DB.Get fail, groupidx %d err: %v", groupIdx, err)
		return nil, err
	}

	if err == ErrNotFoundFromStorage {
		lPLG1Imp("DB.Get not found, groupidx %d", groupIdx)
		return nil, err
	}

	state := &paxospb.AcceptorStateData{}

	err = state.Unmarshal(value)
	if err != nil {
		lPLG1Err(groupIdx, "state.Unmarshal fail, bufferlen %d", len(value))
		return nil, err
	}

	return state, nil
}

func (p *paxosLog) getMaxInstanceIDFromLog(groupIdx int) (uint64, error) {
	instanceID, err := p.logStorage.GetMaxInstanceID(groupIdx)
	if err != nil && err != errMaxInstanceIDNotExist {
		lPLG1Err(groupIdx, "DB.GetMax fail, groupidx %d err: %v", groupIdx, err)
		return 0, err
	}

	if err == errMaxInstanceIDNotExist {
		lPLG1Debug(groupIdx, "MaxInstanceID not exist, groupidx %d", groupIdx)
		return 0, err
	}

	lPLG1Imp(groupIdx, "OK, MaxInstanceID %d groupidsx %d", instanceID, groupIdx)

	return instanceID, err
}
