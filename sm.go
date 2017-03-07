package gopaxos

import "math"

const (
	noCheckpoint = math.MaxUint64 - 1
)

type SMCtx struct {
	SMID int64
	Ctx  interface{}
}

type CheckpointFileInfo struct {
	FilePath string
	FileSize uint64
}

type CheckpointFileInfoList []CheckpointFileInfo

type StateMachine interface {
	//SMID return different SMID which specifies a unique sm.
	SMID() int64

	//Return true means execute success.
	//This 'success' means this execute don't need to retry.
	//Sometimes you will have some logical failure in your execute logic,
	//and this failure will definite occur on all node, that means this failure is acceptable,
	//for this case, return true is the best choice.
	//Some system failure will let different node's execute result inconsistent,
	//for this case, you must return false to retry this execute to avoid this system failure.
	Execute(groupIdx int, instanceID uint64, paxosValue []byte, smCtx *SMCtx) bool

	ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool

	//Only need to implement this function while you have checkpoint.
	//Return your checkpoint's max executed instance id.
	//Notice PhxPaxos will call this function very frequently.
	GetCheckpointInstanceID(groupIdx int) uint64

	//After called this function, the vecFileList that GetCheckpointState return's,
	//can't be deleted, moved and modified.
	LockCheckpointState() error

	//dirPath is checkpoint data root dir path.
	//vecFileList is the relative path of the sDirPath.
	GetCheckpointState(groupIdx int) (string, []string, error)

	UnLockCheckpointState()

	//Checkpoint file was on dir(sCheckpointTmpFileDirPath).
	//vecFileList is all the file in dir(sCheckpointTmpFileDirPath).
	//vecFileList filepath is absolute path.
	//After called this function, paxoslib will kill the processor.
	//State machine need to understand this when restart.
	LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, fileList []string, checkpointInstanceID uint64) error

	//You can modify your request at this moment.
	//At this moment, the state machine data will be up to date.
	//If request is batch, propose requests for multiple identical state machines will only call this function once.
	//Ensure that the execute function correctly recognizes the modified request.
	//Since this function is not always called, the execute function must handle the unmodified request correctly.
	BeforePropose(groupIdx, value []byte) []byte

	//Because function BeforePropose much waste cpu,
	//Only NeedCallBeforePropose return true then will call function BeforePropose.
	//You can use this function to control call frequency.
	//Default is false.
	NeedCallBeforePropose() bool
}
