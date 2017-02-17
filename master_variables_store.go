package paxos

import "github.com/buptmiao/px/paxospb"

type masterVariableStore struct {
	logStorage LogStorage
}

func newMasterVariableStore(ls LogStorage) *masterVariableStore {
	return &masterVariableStore{
		logStorage: ls,
	}
}

func (m *masterVariableStore) write(wo writeOptions, groupIdx int, mVar *paxospb.MasterVariables) error {
	value, err := mVar.Marshal()
	if err != nil {
		lPLG1Err(groupIdx, "Variables.Marshal fail")
		return err
	}

	err = m.logStorage.SetMasterVariables(wo, groupIdx, value)
	if err != nil {
		lPLG1Err(groupIdx, "DB.Put fail, groupidx %d bufferlen %d err: %v",
			groupIdx, len(value), err)
		return err
	}

	return nil
}

func (m *masterVariableStore) read(groupIdx int) (*paxospb.MasterVariables, error) {
	value, err := m.logStorage.GetMasterVariables(groupIdx)
	if err != nil && err != ErrNotFoundFromStorage {
		lPLG1Err(groupIdx, "DB.Get fail, groupidx %d err: %v", groupIdx, err)
		return nil, err
	}

	if err == ErrNotFoundFromStorage {
		lPLG1Imp(groupIdx, "DB.Get not found, groupidx %d", groupIdx)
		return nil, err
	}

	mVar := &paxospb.MasterVariables{}
	err = mVar.Unmarshal(value)
	if err != nil {
		lPLG1Err(groupIdx, "Variables.Unmarshal fail, bufferlen %d", len(value))
		return err
	}

	return nil
}
