package paxos

// default be true
type writeOptions bool

type LogStorage interface {
	GetLogStorageDirPath(groupIdx int) string
	Get(groupIdx int, instanceID uint64) ([]byte, error)
	Put(wo writeOptions, groupIdx int, instanceID uint64, value []byte) error
	Del(wo writeOptions, groupIdx int, instanceID uint64) error
	GetMaxInstanceID(groupIdx int) (uint64, error)
	SetMinChosenInstanceID(wo writeOptions, groupIdx int, minInstanceID uint64) error
	GetMinChosenInstanceID(groupIdx int) (uint64, error)
	ClearAllLog(groupIdx int) error
	SetSystemVariables(wo writeOptions, groupIdx int, value []byte) error
	GetSystemVariables(groupIdx int) ([]byte, error)
	SetMasterVariables(wo writeOptions, groupIdx int, value []byte) error
	GetMasterVariables(groupIdx int) ([]byte, error)
}
