package gopaxos

import (
	"math/rand"
	"sync"

	"github.com/buptmiao/gopaxos/paxospb"
	"math"
)

type masterOperatorType uint32

const (
	masterOperatorType_Complete masterOperatorType = 1
)

// masterStateMachine implements the interface insideSM
type masterStateMachine struct {
	groupIdx      int
	myNodeID      uint64
	mvStore       *masterVariableStore
	masterNodeID  uint64
	masterVersion uint64
	leaseTime     uint32
	absExpireTime uint64
	mu            sync.Mutex
}

func newMasterStateMachine(ls LogStorage, id uint64, groupIdx int) *masterStateMachine {
	return &masterStateMachine{
		mvStore:       newMasterVariableStore(ls),
		groupIdx:      groupIdx,
		myNodeID:      id,
		masterNodeID:  nullNode,
		masterVersion: math.MaxUint64,
		leaseTime:     0,
		absExpireTime: 0,
	}
}

func (m *masterStateMachine) Execute(groupIdx int, instanceID uint64, paxosValue []byte, smCtx *SMCtx) bool {
	masterOper := &paxospb.MasterOperator{}
	err := masterOper.Unmarshal(paxosValue)
	if err != nil {
		lPLGErr(m.groupIdx, "MasterOper data wrong")
		return false
	}

	if masterOper.GetOperator() == uint32(masterOperatorType_Complete) {
		var absMasterTimeout uint64
		if smCtx != nil && smCtx.Ctx != nil {
			absMasterTimeout = smCtx.Ctx.(uint64)
		}

		lPLGImp(m.groupIdx, "absmaster timeout %d", absMasterTimeout)

		if err := m.learnMaster(instanceID, masterOper, absMasterTimeout); err != nil {
			return false
		}
	} else {
		lPLGErr(m.groupIdx, "unknown op %d", masterOper.GetOperator())
		//wrong op, just skip, so return true;
		return true
	}

	return true
}

func (m *masterStateMachine) SMID() int64 {
	return master_V_SMID
}

func (m *masterStateMachine) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool {
	return true
}

func (m *masterStateMachine) GetCheckpointInstanceID(groupIdx int) uint64 {
	return m.masterVersion
}

func (m *masterStateMachine) BeforePropose(groupIdx int, value []byte) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	masterOper := &paxospb.MasterOperator{}
	if err := masterOper.Unmarshal(value); err != nil {
		return value
	}

	masterOper.Lastversion = m.masterVersion
	bytes, err := masterOper.Marshal()
	if err != nil {
		lPLGErr(m.groupIdx, "MasterOper data wrong")
		return value
	}
	return bytes
}

func (m *masterStateMachine) NeedCallBeforePropose() bool {
	return true
}

func (m *masterStateMachine) GetCheckpointState(groupIdx int) (string, []string, error) {
	return "", nil, nil
}

func (m *masterStateMachine) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, fileList []string, checkpointInstanceID uint64) error {
	return nil
}

func (m *masterStateMachine) LockCheckpointState() error {
	return nil
}

func (m *masterStateMachine) UnLockCheckpointState() {
	return
}

func (m *masterStateMachine) init() error {
	value, err := m.mvStore.read(m.groupIdx)
	if err != nil && err != ErrNotFoundFromStorage {
		lPLGErr(m.groupIdx, "Master variables read from store fail, ret %d", err)
		return err
	}

	if err == ErrNotFoundFromStorage {
		lPLGImp(m.groupIdx, "no master variables exist")
	} else {
		m.masterVersion = value.GetVersion()

		if value.GetMasterNodeid() == m.myNodeID {
			m.masterNodeID = nullNode
			m.absExpireTime = 0
		} else {
			m.masterNodeID = value.GetMasterNodeid()
			m.absExpireTime = getSteadyClockMS() + uint64(value.GetLeaseTime())
		}
	}

	lPLGHead(m.groupIdx, "OK, master nodeid %d version %d expiretime %d",
		m.masterNodeID, m.masterVersion, m.absExpireTime)

	return nil
}

func (m *masterStateMachine) learnMaster(instanceID uint64, masterOper *paxospb.MasterOperator, absMasterTimeout uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lPLGDebug(m.groupIdx, "my last version %d other last version %d this version %d instanceid %d",
		m.masterVersion, masterOper.GetLastversion(), masterOper.GetVersion(), instanceID)

	if masterOper.GetLastversion() != 0 && instanceID > m.masterVersion && masterOper.GetLastversion() != m.masterVersion {
		getBPInstance().MasterSMInconsistent()

		lPLGErr(m.groupIdx, "other last version %d not same to my last version %d, instanceid %d",
			masterOper.GetLastversion(), m.masterVersion, instanceID)

		if rand.Uint32()%100 < 50 {
			//try to fix online
			lPLGErr(m.groupIdx, "try to fix, set my master version %d as other last version %d, instanceid %d",
				m.masterVersion, masterOper.GetLastversion(), instanceID)

			m.masterVersion = masterOper.GetLastversion()
		}
	}

	if masterOper.GetVersion() != m.masterVersion {
		lPLGDebug(m.groupIdx, "version conflit, op version %d now master version %d",
			masterOper.GetVersion(), m.masterVersion)
		return nil
	}

	if err := m.updateMasterToStore(masterOper.GetNodeid(), instanceID, uint32(masterOper.GetTimeout())); err != nil {
		lPLGErr(m.groupIdx, "UpdateMasterToStore fail, err %v", err)
		return err
	}

	m.masterNodeID = masterOper.GetNodeid()
	if m.masterNodeID == m.myNodeID {
		//self be master
		//use local abstimeout
		m.absExpireTime = absMasterTimeout
		getBPInstance().SuccessBeMaster()
		lPLGHead(m.groupIdx, "Be master success, absexpiretime %d", m.absExpireTime)
	} else {
		//other be master
		//use new start timeout
		m.absExpireTime = getSteadyClockMS() + uint64(masterOper.GetTimeout())

		getBPInstance().OtherBeMaster()
		lPLGHead(m.groupIdx, "Ohter be master, absexpiretime %d", m.absExpireTime)
	}

	m.leaseTime = uint32(masterOper.GetTimeout())
	m.masterVersion = instanceID

	lPLGImp(m.groupIdx, "OK, masternodeid %d version %d abstimeout %d",
		m.masterNodeID, m.masterVersion, m.absExpireTime)

	return nil
}

func (m *masterStateMachine) getMaster() uint64 {
	if getSteadyClockMS() >= m.absExpireTime {
		return nullNode
	}

	return m.masterNodeID
}

func (m *masterStateMachine) getMasterWithVersion() (uint64, uint64) {
	return m.safeGetMaster()
}

func (m *masterStateMachine) isIMMaster() bool {
	return m.getMaster() == m.myNodeID
}

func (m *masterStateMachine) updateMasterToStore(masterNodeID uint64, version uint64, leaseTime uint32) error {
	value := &paxospb.MasterVariables{}
	value.MasterNodeid = masterNodeID
	value.Version = version
	value.LeaseTime = leaseTime

	wo := writeOptions(true)

	return m.mvStore.write(wo, m.groupIdx, value)
}

func (m *masterStateMachine) safeGetMaster() (uint64, uint64) {
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

	if m.masterVersion == math.MaxUint64 {
		return nil, nil
	}

	mVar := &paxospb.MasterVariables{}
	mVar.MasterNodeid = m.masterNodeID
	mVar.Version = m.masterVersion
	mVar.LeaseTime = m.leaseTime

	bytes, err := mVar.Marshal()
	if err != nil {
		lPLGErr(m.groupIdx, "Variables.Marshal fail")
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
		lPLGErr(m.groupIdx, "Variables.Unmarshal fail, bufferlen %d", len(buf))
		return false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if mVar.GetVersion() <= m.masterVersion && m.masterVersion != math.MaxUint64 {
		lPLGImp(m.groupIdx, "lag checkpoint, no need update, cp.version %d now.version %d",
			mVar.GetVersion(), m.masterVersion)
		return false, nil
	}

	err = m.updateMasterToStore(mVar.GetMasterNodeid(), mVar.GetVersion(), mVar.GetLeaseTime())
	if err != nil {
		return false, err
	}

	lPLGHead(m.groupIdx, "ok, cp.version %d cp.masternodeid %d old.version %d old.masternodeid %d",
		mVar.GetVersion(), mVar.GetMasterNodeid(), m.masterVersion, m.masterNodeID)

	m.masterVersion = mVar.GetVersion()

	if mVar.GetMasterNodeid() == m.myNodeID {
		m.masterNodeID = nullNode
		m.absExpireTime = 0
	} else {
		m.masterNodeID = mVar.GetMasterNodeid()
		m.absExpireTime = getSteadyClockMS() + uint64(mVar.GetLeaseTime())
	}

	return false, nil
}
