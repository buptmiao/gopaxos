package gopaxos

import (
	"encoding/binary"
	"github.com/buptmiao/gopaxos/paxospb"
)

type batchSMCtx struct {
	smCtxList []*SMCtx
}

type smFac struct {
	smList   []StateMachine
	groupIdx int
}

func newSMFac(groupIdx int) *smFac {
	return &smFac{
		groupIdx: groupIdx,
	}
}

func (s *smFac) execute(groupIdx int, instanceID uint64, paxosValue []byte, ctx *SMCtx) bool {
	if len(paxosValue) < 8 {
		lPLGErr(s.groupIdx, "Value wrong, instanceid %d size %d", instanceID, len(paxosValue))
		return true
	}

	smID := binary.LittleEndian.Uint64(paxosValue)
	if smID == 0 {
		lPLGImp("Value no need to do sm, just skip, instanceid %d", instanceID)
		return true
	}

	bodyValue := paxosValue[8:]

	if smID == batch_Propose_SMID {
		var batSMCtx *batchSMCtx
		if ctx != nil && ctx.Ctx != nil {
			batSMCtx = ctx.Ctx.(*batchSMCtx)
		}
		return s.batchExecute(groupIdx, instanceID, bodyValue, batSMCtx)
	}

	return s.doExecute(groupIdx, instanceID, bodyValue, smID, ctx)
}

func (s *smFac) batchExecute(groupIdx int, instanceID uint64, bodyValue []byte, ctx *batchSMCtx) bool {
	batchValue := &paxospb.BatchPaxosValues{}
	err := batchValue.Unmarshal(bodyValue)
	if err != nil {
		lPLGErr("Unmarshal fail, valuesize %d", len(bodyValue))
		return false
	}

	if ctx != nil {
		if len(ctx.smCtxList) != len(batchValue.GetValues()) {
			lPLGErr("values size %d not equal to smctx size %d",
				len(batchValue.GetValues()), len(ctx.smCtxList))
			return false
		}
	}

	for i := 0; i < len(batchValue.GetValues()); i++ {
		value := batchValue.GetValues()[i]
		var c *SMCtx
		if ctx != nil {
			c = ctx.smCtxList[i]
		}

		if !s.doExecute(groupIdx, instanceID, value.GetValue(), value.GetSMID(), c) {
			return false
		}
	}

	return true
}

func (s *smFac) doExecute(groupIdx int, instanceID uint64, value []byte, smID int64, ctx *SMCtx) bool {
	if smID == 0 {
		lPLGImp("Value no need to do sm, just skip, instanceid %d", instanceID)
		return true
	}

	if len(s.smList) == 0 {
		lPLGImp("No any sm, need wait sm, instanceid %d", instanceID)
		return false
	}

	for _, sm := range s.smList {
		if sm.SMID() == smID {
			return sm.Execute(groupIdx, instanceID, value, ctx)
		}
	}

	lPLGErr("Unknown smid %d instanceid %d", smID, instanceID)
	return false
}

func (s *smFac) executeForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool {
	if len(paxosValue) < 8 {
		lPLGErr("Value wrong, instanceid %d size %d", instanceID, len(paxosValue))
		//need do nothing, just skip
		return true
	}

	smID := binary.LittleEndian.Uint64(paxosValue)
	if smID == 0 {
		lPLGImp("Value no need to do sm, just skip, instanceid %d", instanceID)
		return true
	}

	bodyValue := paxosValue[8:]
	if smID == batch_Propose_SMID {
		s.batchExecuteForCheckpoint(groupIdx, instanceID, bodyValue)
	}

	return s.doExecuteForCheckpoint(groupIdx, instanceID, bodyValue, smID)
}

func (s *smFac) batchExecuteForCheckpoint(groupIdx int, instanceID uint64, bodyValue []byte) bool {
	batchValue := &paxospb.BatchPaxosValues{}
	err := batchValue.Unmarshal(bodyValue)
	if err != nil {
		lPLGErr("Unmarshal fail, valuesize %d", len(bodyValue))
		return false
	}

	for i := 0; i < len(batchValue.GetValues()); i++ {
		value := batchValue.GetValues()[i]
		if !s.doExecuteForCheckpoint(groupIdx, instanceID, value.GetValue(), value.GetSMID()) {
			return false
		}
	}

	return true
}

func (s *smFac) doExecuteForCheckpoint(groupIdx int, instanceID uint64, bodyValue []byte, smID int64) bool {
	if smID == 0 {
		lPLGImp("Value no need to do sm, just skip, instanceid %d", instanceID)
		return true
	}

	if len(s.smList) == 0 {
		lPLGImp("No any sm, need wait sm, instanceid %d", instanceID)
		return false
	}

	for _, sm := range s.smList {
		if sm.SMID() == smID {
			return sm.ExecuteForCheckpoint(groupIdx, instanceID, bodyValue)
		}
	}

	lPLGErr("Unknown smid %d instanceid %d", smID, instanceID)
	return false
}

func (s *smFac) packPaxosValue(paxosValue []byte, smID int64) []byte {
	buf := make([]byte, 8, 8+len(paxosValue))
	binary.LittleEndian.PutUint64(buf, uint64(smID))
	return append(buf, paxosValue...)
}

func (s *smFac) addSM(sm StateMachine) {
	for _, s := range s.smList {
		if s.SMID() == sm.SMID() {
			return
		}
	}
	s.smList = append(s.smList, sm)
}

func (s *smFac) getCheckpointInstanceID(groupIdx int) uint64 {

	cpInstanceID, cpInstanceIDInsize, haveUseSM := -1, -1, false
	for _, sm := range s.smList {
		checkpointInstanceID := sm.GetCheckpointInstanceID(groupIdx)
		if sm.SMID() == system_V_SMID || sm.SMID() == master_V_SMID {
			//system variables
			//master variables
			//if no user state machine, system and master's can use.
			//if have user state machine, use user'state machine's checkpointinstanceid.
			if checkpointInstanceID == uint64(-1) {
				continue
			}

			if checkpointInstanceID > cpInstanceIDInsize || cpInstanceIDInsize == uint64(-1) {
				cpInstanceIDInsize = checkpointInstanceID
			}

			continue
		}

		haveUseSM = true

		if checkpointInstanceID == uint64(-1) {
			continue
		}

		if checkpointInstanceID > cpInstanceID || cpInstanceID == uint64(-1) {
			cpInstanceID = checkpointInstanceID
		}
	}

	if haveUseSM {
		return cpInstanceID
	}
	return cpInstanceIDInsize
}

func (s *smFac) getSMList() []StateMachine {
	return s.smList
}

func (s *smFac) beforePropose(groupIdx int, value []byte) []byte {
	smID := binary.LittleEndian.Uint64(value)
	if smID == 0 {
		return value
	}

	if smID == batch_Propose_SMID {
		return s.beforeBatchPropose(groupIdx, value)
	}
	bodyValue := value[8:]
	newValue, change := s.beforeProposeCall(groupIdx, smID, bodyValue)
	if change {
		return append(value[:8], newValue...)
	}

	return value
}

func (s *smFac) beforeBatchPropose(groupIdx int, value []byte) []byte {
	batchValue := &paxospb.BatchPaxosValues{}
	err := batchValue.Unmarshal(value[8:])
	if err != nil {
		return value
	}

	var change bool
	smAlreadyCall := make(map[int64]struct{})
	for i := 0; i < len(batchValue.GetValues()); i++ {
		v := batchValue.GetValues()[i]

		if _, ok := smAlreadyCall[v.GetSMID()]; ok {
			continue
		}

		smAlreadyCall[v.GetSMID()] = struct{}{}

		v.Value, change = s.beforeProposeCall(groupIdx, v.GetSMID(), v.GetValue())
	}

	if change {
		data, err := batchValue.Marshal()
		if err != nil {
			lPLGErr(groupIdx, "Marshal error %v", err)
			return value
		}
		return append(value[:8], data...)
	}

	return value
}

func (s *smFac) beforeProposeCall(groupIdx int, smID int64, bodyValue []byte) ([]byte, bool) {
	if smID == 0 || len(s.smList) == 0 {
		return bodyValue, false
	}

	for _, sm := range s.smList {
		if sm.SMID() == smID {
			if sm.NeedCallBeforePropose() {
				return sm.BeforePropose(groupIdx, bodyValue), true
			}
		}
	}

	return bodyValue, false
}
