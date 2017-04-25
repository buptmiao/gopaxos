package gopaxos

// default be true
type WriteOptions bool

type LogStorage interface {
	GetLogStorageDirPath(groupIdx int) string
	Get(groupIdx int, instanceID uint64) ([]byte, error)
	Put(wo WriteOptions, groupIdx int, instanceID uint64, value []byte) error
	Del(wo WriteOptions, groupIdx int, instanceID uint64) error
	GetMaxInstanceID(groupIdx int) (uint64, error)
	SetMinChosenInstanceID(wo WriteOptions, groupIdx int, minInstanceID uint64) error
	GetMinChosenInstanceID(groupIdx int) (uint64, error)
	ClearAllLog(groupIdx int) error
	SetSystemVariables(wo WriteOptions, groupIdx int, value []byte) error
	GetSystemVariables(groupIdx int) ([]byte, error)
	SetMasterVariables(wo WriteOptions, groupIdx int, value []byte) error
	GetMasterVariables(groupIdx int) ([]byte, error)
}
