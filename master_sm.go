package paxos

import (
	"math/rand"
	"sync"

	"github.com/buptmiao/px/paxospb"
)

type masterOperatorType uint32

const (
	masterOperatorType_Complete masterOperatorType = 1
)

// masterStateMachine implements the interface insideSM
type masterStateMachine struct {
	groupIdx      int
	myNodeID      nodeId
	mvStore       *masterVariableStore
	masterNodeID  nodeId
	masterVersion uint64
	leaseTime     int
	absExpireTime uint64
	mu            sync.Mutex
}

func newMasterStateMachine(ls LogStorage, id nodeId, groupIdx int) *masterStateMachine {
	return &masterStateMachine{
		mvStore:       newMasterVariableStore(ls),
		groupIdx:      groupIdx,
		myNodeID:      id,
		masterNodeID:  nullNode,
		masterVersion: uint64(-1),
		leaseTime:     0,
		absExpireTime: 0,
	}
}

func (m *masterStateMachine) Execute(groupIdx int, instanceID uint64, paxosValue []byte, smCtx *SMCtx) bool {
	masterOper := &paxospb.MasterOperator{}
	err := masterOper.Unmarshal(paxosValue)
	if err != nil {
		lPLG1Err(m.groupIdx, "MasterOper data wrong")
		return false
	}

	if masterOper.GetOperator() == masterOperatorType_Complete {
		var absMasterTimeout uint64
		if smCtx != nil && smCtx.Ctx != nil {
			absMasterTimeout = smCtx.Ctx.(uint64)
		}

		lPLG1Imp(m.groupIdx, "absmaster timeout %d", absMasterTimeout)

		if err := m.learnMaster(instanceID, masterOper, absMasterTimeout); err != nil {
			return false
		}
	} else {
		lPLG1Err(m.groupIdx, "unknown op %d", masterOper.GetOperator())
		//wrong op, just skip, so return true;
		return true
	}

	return true
}

func (m *masterStateMachine) SMID() int64 {
	return master_v_smid
}

func (m *masterStateMachine) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool {
	return true
}

func (m *masterStateMachine) GetCheckpointInstanceID(groupIdx int) uint64 {
	return m.masterVersion
}

func (m *masterStateMachine) BeforePropose(groupIdx, value []byte) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	masterOper := &paxospb.MasterOperator{}
	if err := masterOper.Unmarshal(value); err != nil {
		return value
	}

	masterOper.Lastversion = m.masterVersion
	bytes, err := masterOper.Marshal()
	if err != nil {
		lPLG1Err(m.groupIdx, "MasterOper data wrong")
		return value
	}
	return bytes
}

func (m *masterStateMachine) NeedCallBeforePropose() bool {
	return true
}

func (m *masterStateMachine) GetCheckpointState(groupIdx int, dirPath string) ([]string, error) {
	return nil, nil
}

func (m *masterStateMachine) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, fileList []string, checkpointInstanceID uint64) error {
	return nil
}

func (m *masterStateMachine) LockCheckpointState() error {
	return nil
}

func (m *masterStateMachine) UnLockCheckpointState() {

}

func (m *masterStateMachine) init() error {
	value, err := m.mvStore.read(m.groupIdx)
	if err != nil && err != ErrNotFoundFromStorage {
		lPLG1Err(m.groupIdx, "Master variables read from store fail, ret %d", err)
		return err
	}

	if err == ErrNotFoundFromStorage {
		lPLG1Imp(m.groupIdx, "no master variables exist")
	} else {
		m.masterVersion = value.GetVersion()

		if value.GetMasterNodeid() == m.myNodeID {
			m.masterNodeID = nullNode
			m.absExpireTime = 0
		} else {
			m.masterNodeID = value.GetMasterNodeid()
			m.absExpireTime = getSteadyClockMS() + value.GetLeaseTime()
		}
	}

	lPLG1Head(m.groupIdx, "OK, master nodeid %d version %d expiretime %d",
		m.masterNodeID, m.masterVersion, m.absExpireTime)

	return nil
}

func (m *masterStateMachine) learnMaster(instanceID uint64, masterOper *paxospb.MasterOperator, absMasterTimeout uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lPLG1Debug(m.groupIdx, "my last version %d other last version %d this version %d instanceid %d",
		m.masterVersion, masterOper.GetLastversion(), masterOper.GetVersion(), instanceID)

	if masterOper.GetLastversion() != 0 && instanceID > m.masterVersion && masterOper.GetLastversion() != m.masterVersion {
		getBPInstance().MasterSMInconsistent()

		lPLG1Err(m.groupIdx, "other last version %d not same to my last version %d, instanceid %d",
			masterOper.GetLastversion(), m.masterVersion, instanceID)

		if rand.Uint32()%100 < 50 {
			//try to fix online
			lPLG1Err(m.groupIdx, "try to fix, set my master version %d as other last version %d, instanceid %d",
				m.masterVersion, masterOper.GetLastversion(), instanceID)

			m.masterVersion = masterOper.GetLastversion()
		}
	}

	if masterOper.GetVersion() != m.masterVersion {
		lPLG1Debug(m.groupIdx, "version conflit, op version %d now master version %d",
			masterOper.GetVersion(), m.masterVersion)
		return nil
	}

	if err := m.updateMasterToStore(masterOper.GetNodeid(), instanceID, masterOper.GetTimeout()); err != nil {
		lPLG1Err(m.groupIdx, "UpdateMasterToStore fail, err %v", err)
		return err
	}

	m.masterNodeID = masterOper.GetNodeid()
	if m.masterNodeID == m.myNodeID {
		//self be master
		//use local abstimeout
		m.absExpireTime = absMasterTimeout
		getBPInstance().SuccessBeMaster()
		lPLG1Head(m.groupIdx, "Be master success, absexpiretime %d", m.absExpireTime)
	} else {
		//other be master
		//use new start timeout
		m.absExpireTime = getSteadyClockMS() + masterOper.GetTimeout()

		getBPInstance().OtherBeMaster()
		lPLG1Head(m.groupIdx, "Ohter be master, absexpiretime %d", m.absExpireTime)
	}

	m.leaseTime = masterOper.GetTimeout()
	m.masterVersion = instanceID

	lPLG1Imp(m.groupIdx, "OK, masternodeid %d version %d abstimeout %d",
		m.masterNodeID, m.masterVersion, m.absExpireTime)

	return nil
}

func (m *masterStateMachine) getMaster() nodeId {
	if getSteadyClockMS() >= m.absExpireTime {
		return nullNode
	}

	return m.masterNodeID
}

func (m *masterStateMachine) getMasterWithVersion() (nodeId, uint64) {
	return m.safeGetMaster()
}

func (m *masterStateMachine) isIMMaster() bool {
	return m.getMaster() == m.myNodeID
}

func (m *masterStateMachine) updateMasterToStore(masterNodeID nodeId, version uint64, leaseTime uint32) error {
	value := &paxospb.MasterVariables{}
	value.MasterNodeid = masterNodeID
	value.Version = version
	value.LeaseTime = leaseTime

	wo := writeOptions(true)

	return m.mvStore.write(wo, m.groupIdx, value)
}

func (m *masterStateMachine) safeGetMaster() (nodeId, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if getSteadyClockMS() >= m.absExpireTime {
		return nullNode, m.masterVersion
	} else {
		return m.masterNodeID, m.masterVersion
	}
}

func (m *masterStateMachine) GetCheckpointBuffer() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.masterVersion == uint64(-1) {
		return nil, nil
	}

	mVar := &paxospb.MasterVariables{}
	mVar.MasterNodeid = m.masterNodeID
	mVar.Version = m.masterVersion
	mVar.LeaseTime = m.leaseTime

	bytes, err := mVar.Marshal()
	if err != nil {
		lPLG1Err(m.groupIdx, "Variables.Marshal fail")
		return nil, err
	}

	return bytes, nil
}

// the first return value is invalid
func (m *masterStateMachine) UpdateByCheckpoint(buf []byte) (bool, error) {
	if len(buf) == 0 {
		return false, nil
	}

	mVar := &paxospb.MasterVariables{}
	err := mVar.Unmarshal(buf)
	if err != nil {
		lPLG1Err(m.groupIdx, "Variables.Unmarshal fail, bufferlen %d", len(buf))
		return false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if mVar.GetVersion() <= m.masterVersion && m.masterVersion != uint64(-1) {
		lPLG1Imp(m.groupIdx, "lag checkpoint, no need update, cp.version %d now.version %d",
			mVar.GetVersion(), m.masterVersion)
		return false, nil
	}

	err = m.updateMasterToStore(mVar.GetMasterNodeid(), mVar.GetVersion(), mVar.GetLeaseTime())
	if err != nil {
		return false, err
	}

	lPLG1Head(m.groupIdx, "ok, cp.version %d cp.masternodeid %d old.version %d old.masternodeid %d",
		mVar.GetVersion(), mVar.GetMasterNodeid(), m.masterVersion, m.masterNodeID)

	m.masterVersion = mVar.GetVersion()

	if mVar.GetMasterNodeid() == m.myNodeID {
		m.masterNodeID = nullNode
		m.absExpireTime = 0
	} else {
		m.masterNodeID = mVar.GetMasterNodeid()
		m.absExpireTime = getSteadyClockMS() + mVar.GetLeaseTime()
	}

	return false, nil
}
