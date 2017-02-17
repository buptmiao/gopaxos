package gopaxos

import (
	"github.com/buptmiao/gopaxos/paxospb"
)

type systemVariableStore struct {
	logStorage LogStorage
}

func newSystemVariableStore(ls LogStorage) *systemVariableStore {
	return &systemVariableStore{
		logStorage: ls,
	}
}

func (s *systemVariableStore) write(wo writeOptions, groupIdx int, sysVar *paxospb.SystemVariables) error {
	value, err := sysVar.Marshal()
	if err != nil {
		lPLG1Err(groupIdx, "Variables.Marshal fail")
		return err
	}

	err = s.logStorage.SetSystemVariables(wo, groupIdx, value)
	if err != nil {
		lPLG1Err(groupIdx, "DB.Put fail, groupidx %d bufferlen %d ret %v",
			groupIdx, len(value), err)
		return err
	}

	return nil
}

func (s *systemVariableStore) read(groupIdx int) (*paxospb.SystemVariables, error) {
	value, err := s.logStorage.GetSystemVariables(groupIdx)
	if err != nil && err != ErrNotFoundFromStorage {
		lPLG1Err(groupIdx, "DB.Get fail, groupidx %d err: %v", groupIdx, err)
		return nil, err
	}

	if err == ErrNotFoundFromStorage {
		lPLG1Imp(groupIdx, "DB.Get not found, groupidx %d", groupIdx)
		return nil, err
	}

	sysVar := &paxospb.SystemVariables{}
	err = sysVar.Unmarshal(value)
	if err != nil {
		lPLG1Err(groupIdx, "Variables.Unmarshal fail, bufferlen %d", len(value))
		return err
	}

	return nil
}
