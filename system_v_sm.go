package gopaxos

import (
	"github.com/buptmiao/gopaxos/paxospb"
)

type insideSM interface {
	StateMachine
	GetCheckpointBuffer() ([]byte, error)
	UpdateByCheckpoint(buf []byte) (bool, error)
}

// systemVSM implements interface insideSM.
type systemVSM struct {
	groupIdx  int
	sysVar    *paxospb.SystemVariables
	sysVStore *systemVariableStore
	nodeIDSet map[nodeId]struct{}
	myNodeID  nodeId
	cb        MembershipChangeCallback
}

func newSystemVSM(groupIdx int, id nodeId, ls LogStorage, cb MembershipChangeCallback) *systemVSM {
	return &systemVSM{
		groupIdx:  groupIdx,
		sysVStore: newSystemVariableStore(ls),
		myNodeID:  id,
		cb:        cb,
	}
}

func (s *systemVSM) init() error {
	var err error
	s.sysVar, err = s.sysVStore.read(s.groupIdx)
	if err != nil || err != ErrNotFoundFromStorage {
		return err
	}

	if err == ErrNotFoundFromStorage {
		s.sysVar.Gid = 0
		s.sysVar.Version = -1
		lPLGImp(s.groupIdx, "variables not exist")
	} else {
		s.refreshNodeID()
		lPLGImp(s.groupIdx, "OK, gourpidx %d gid %d version %d",
			s.sysVar.GetGid(), s.sysVar.GetVersion())
	}

	return nil
}

func (s *systemVSM) updateSystemVariables(sysVar *paxospb.SystemVariables) error {
	wo := writeOptions(true)

	if err := s.sysVStore.write(wo, s.groupIdx, sysVar); err != nil {
		lPLGErr(s.groupIdx, "SystemVStore::Write fail, ret %v", err)
		return err
	}

	s.sysVar = sysVar

	s.refreshNodeID()

	return nil
}

func (s *systemVSM) Execute(groupIdx int, instanceID uint64, value []byte, ctx *SMCtx) bool {
	sysVar := &paxospb.SystemVariables{}
	err := sysVar.Unmarshal(value)
	if err != nil {
		lPLGErr(s.groupIdx, "Variables.ParseFromArray fail, bufferlen %d", len(value))
		return false
	}

	if s.sysVar.GetGid() != 0 && sysVar.GetGid() != s.sysVar.GetGid() {
		lPLGErr(s.groupIdx, "modify.gid %d not equal to now.gid %d", sysVar.GetGid(), s.sysVar.GetGid())
		if ctx != nil {
			ctx.Ctx = paxos_MembershipOp_GidNotSame
		}
		return true
	}

	if sysVar.GetVersion() != s.sysVar.GetVersion() {
		lPLGErr("modify.version %d not equal to now.version %d", sysVar.GetVersion(), s.sysVar.GetVersion())
		if ctx != nil {
			ctx.Ctx = paxos_MembershipOp_VersionConflit
		}
		return true
	}

	sysVar.Version = instanceID
	err = s.updateSystemVariables(sysVar)
	if err != nil {
		return false
	}

	lPLGHead(groupIdx, "OK, new version %d gid %d", s.sysVar.GetVersion(), s.sysVar.GetGid())
	if ctx != nil {
		ctx.Ctx = 0
	}

	return true
}

func (s *systemVSM) getGid() uint64 {
	return s.sysVar.GetGid()
}

func (s *systemVSM) getMembership() (NodeInfoList, uint64) {
	version := s.sysVar.GetVersion()
	members := make(NodeInfoList, 0, len(s.sysVar.GetMemberShip()))
	for _, n := range s.sysVar.GetMemberShip() {
		nodeInfo := NewNodeInfo(n.GetNodeid(), "", 0)
		nodeInfo.parseNodeID()
		members = append(members, nodeInfo)
	}

	return members, version
}

func (s *systemVSM) membershipOPValue(nodeList NodeInfoList, version uint64) ([]byte, error) {
	sysVar := &paxospb.SystemVariables{}
	sysVar.Version = version
	sysVar.Gid = s.getGid()

	for _, n := range nodeList {
		member := paxospb.PaxosNodeInfo{}
		member.Nodeid = n.GetNodeID()
		member.Rid = 0
		sysVar.MemberShip = append(sysVar.MemberShip, member)
	}

	bytes, err := sysVar.Marshal()
	if err != nil {
		lPLGErr(s.groupIdx, "Variables.Marshal fail")
		return nil, err
	}

	return bytes, nil
}

func (s *systemVSM) createGidOPValue(gid uint64) ([]byte, error) {
	sysVar := &paxospb.SystemVariables{}
	sysVar.Gid = gid

	bytes, err := sysVar.Marshal()
	if err != nil {
		lPLGErr(s.groupIdx, "Variables.Marshal fail")
		return nil, err
	}

	return bytes, nil
}

func (s *systemVSM) addNodeIDList(nodeList NodeInfoList) {
	if s.sysVar.GetGid() != 0 {
		lPLGErr(s.groupIdx, "No need to add, i already have membership info.")
		return
	}

	s.nodeIDSet = make(map[nodeId]struct{})
	s.sysVar.MemberShip = nil

	for _, n := range nodeList {
		member := paxospb.PaxosNodeInfo{}
		member.Nodeid = n.GetNodeID()
		member.Rid = 0
		s.sysVar.MemberShip = append(s.sysVar.MemberShip, member)
	}

	s.refreshNodeID()
}

func (s *systemVSM) refreshNodeID() {
	s.nodeIDSet = make(map[nodeId]struct{})
	nodeList := make(NodeInfoList, 0, len(s.sysVar.GetMemberShip()))

	for _, n := range s.sysVar.GetMemberShip() {
		nodeInfo := NewNodeInfo(n.GetNodeid(), "", 0)
		nodeInfo.parseNodeID()

		lPLGHead("ip %s port %d nodeid %d",
			nodeInfo.GetIP(), nodeInfo.GetPort(), nodeInfo.GetNodeID())
		s.nodeIDSet[nodeInfo.GetNodeID()]

		nodeList = append(nodeList, nodeInfo)
	}

	if s.cb != nil {
		s.cb(s.groupIdx, nodeList)
	}
}

func (s *systemVSM) getNodeCount() int {
	return len(s.nodeIDSet)
}

func (s *systemVSM) getMajorityCount() int {
	if s.getNodeCount()%2 == 1 {
		return (s.getNodeCount() + 1) >> 1
	}
	return s.getNodeCount()>>1 + 1
}

func (s *systemVSM) isValidNodeID(id nodeId) bool {
	if s.sysVar.GetGid() == 0 {
		return true
	}

	_, ok := s.nodeIDSet[id]
	return ok
}

func (s *systemVSM) isIMInMembership() bool {
	_, ok := s.nodeIDSet[s.myNodeID]
	return ok
}

func (s *systemVSM) GetCheckpointBuffer() ([]byte, error) {
	if s.sysVar.GetVersion() == uint64(-1) || s.sysVar.GetGid() == 0 {
		return nil, nil
	}
	bytes, err := s.sysVar.Marshal()
	if err != nil {
		lPLGErr(s.groupIdx, "Variables.Marshal fail")
		return nil, err
	}
	return bytes, nil
}

func (s *systemVSM) UpdateByCheckpoint(buf []byte) (bool, error) {
	if len(buf) == 0 {
		return false, nil
	}

	sysVar := &paxospb.SystemVariables{}
	if err := sysVar.Unmarshal(buf); err != nil {
		lPLGErr("Variables.Unmarshal fail, bufferlen %d", len(buf))
		return false, err
	}

	if sysVar.GetVersion() == uint64(-1) {
		lPLGErr(s.groupIdx, "variables.version not init, this is not checkpoint")
		return false, errVersionNotInit
	}

	if s.sysVar.GetGid() != 0 && sysVar.GetGid() != s.sysVar.GetGid() {
		lPLGErr("gid not same, cp.gid %d now.gid %d", sysVar.GetGid(), s.sysVar.GetGid())
		return false, errGidNotSame
	}

	if s.sysVar.GetVersion() != uint64(-1) && sysVar.GetVersion() <= s.sysVar.GetVersion() {
		lPLGImp("lag checkpoint, no need update, cp.version %d now.version %d",
			sysVar.GetVersion(), s.sysVar.GetVersion())
		return false, nil
	}
	oldVar := *s.sysVar
	err := s.updateSystemVariables(sysVar)
	if err != nil {
		return true, err
	}

	lPLGHead("ok, cp.version %d cp.membercount %d old.version %d old.membercount %d",
		sysVar.GetVersion(), len(sysVar.GetMemberShip()), oldVar.GetVersion(), len(oldVar.GetMemberShip()))

	return true, nil
}

func (s *systemVSM) getSystemVariables() *paxospb.SystemVariables {
	return s.sysVar
}

func (s *systemVSM) getMembershipMap() map[nodeId]struct{} {
	return s.nodeIDSet
}

func (s *systemVSM) SMID() int {
	return system_V_SMID
}

func (s *systemVSM) GetCheckpointInstanceID(groupIdx int) uint64 {
	return s.sysVar.GetVersion()
}

func (s *systemVSM) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool {
	return true
}

func (s *systemVSM) GetCheckpointState(groupIdx int) (string, []string, error) {
	return nil, nil, nil
}

func (s *systemVSM) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, fileList []string, checkpointInstanceID uint64) error {
	return nil
}

func (s *systemVSM) LockCheckpointState() error {
	return nil
}

func (s *systemVSM) UnLockCheckpointState() {
	return
}

func (s *systemVSM) BeforePropose(groupIdx, value []byte) []byte {
	return nil
}

func (s *systemVSM) NeedCallBeforePropose() bool {
	return false
}
