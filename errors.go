package gopaxos

import (
	"errors"
)

type paxosError struct {
	code int32
	msg  string
}

func newPaxosError(code int32, msg string) *paxosError {
	return &paxosError{
		code: code,
		msg:  msg,
	}
}

func (p *paxosError) Error() string {
	return p.msg
}

var (
	ErrLogStoragePathEmpty = errors.New("empty path of log storage")
	ErrNotFoundFromStorage = errors.New("Not found from storage")
	ErrUDPMaxSizeTooLarge  = errors.New("udp max size too large")
	ErrGroupCount          = errors.New("group count too proper")
	ErrSelfNodeFollowed    = errors.New("self node follow itself")
	ErrGroupIdxOutOfRange  = errors.New("group index out of range")

	errChecksumNotMatch             = errors.New("checksum error")
	errFileBroken                   = errors.New("file is broken")
	errMetaFileBroken               = errors.New("meta file broken")
	errGetFileID                    = errors.New("get file id failed")
	errFileIDTooLarge               = errors.New("del fileid too large")
	errFileIDMismatch               = errors.New("fileid mismatch")
	errFileNotExist                 = errors.New("file not exists")
	errFileSizeWrong                = errors.New("file size is wrong")
	errDBNotInit                    = errors.New("db not init yet")
	errFileInstanceIDMismatch       = errors.New("file instanceid not match")
	errInstanceIDSizeWrong          = errors.New("instance id size wrong")
	errGroupCountNotProper          = errors.New("group count wrong")
	errMaxInstanceIDNotExist        = errors.New("max instance id not exists")
	errMsgQueueFull                 = errors.New("msg queue is full")
	errVersionNotInit               = errors.New("version not init")
	errGidNotSame                   = errors.New("gid not same")
	errCheckpointInstanceID         = errors.New("checkpoint instanceID is not correct")
	errSmallThanMinChosenInstanceID = errors.New("small than min chosen instanceid")
	errInstanceExecuteFailed        = errors.New("failure in instance execution")
	errGetInstanceValueNotExist     = errors.New("paxos getinstancevalue value not exist")
	errGetInstanceValueNotChosen    = errors.New("paxos getinstancevalue value not chosen yet")
	errCheckpointMissMajority       = errors.New("Need more other tell us need to askforcheckpoint")
	errMsgNotValid                  = errors.New("msg not valid")
	errMsgSequenceWrong             = errors.New("msg sequence wrong")
	errMsgSizeTooSmall              = errors.New("msg size too small")
	errFileOffsetMismatch           = errors.New("file offset mismatch")
	errCheckpointSenderEnded        = errors.New("checkpoint sender has beed ended")

	errCheckpointAck     = errors.New("checkpoint sender ack check error")
	errHeaderLenTooLong  = errors.New("headerlen too long")
	errChecksumMiss      = errors.New("checksum size error")
	errReceiverNotFinish = errors.New("cp receiver not finished")
	errQueueMemExceed    = errors.New("msg queue mem size too large")

	errPaxosSystemError                = newPaxosError(int32(paxos_SystemError), "system error")
	errGroupIdxWrong                   = newPaxosError(int32(paxos_GroupIdxWrong), "group index wrong")
	errMembershipOpGidNotSame          = newPaxosError(int32(paxos_MembershipOp_GidNotSame), "membership operation, gid not same")
	errMembershipOpVersionConflict     = newPaxosError(int32(paxos_MembershipOp_VersionConflict), "membership operation, version conflict")
	errMembershipOpNoGid               = newPaxosError(int32(paxos_MembershipOp_NoGid), "membership operation, no gid")
	errMembershipOpNodeExists          = newPaxosError(int32(paxos_MembershipOp_Add_NodeExist), "membership operation, node already exists")
	errMembershipOpRemoveNodeNotExists = newPaxosError(int32(paxos_MembershipOp_Remove_NodeNotExist), "membership operation, node does not exists")
	errMembershipOpNoChange            = newPaxosError(int32(paxos_MembershipOp_Change_NoChange), "membership operation, no changes happen")
	errValueNotExists                  = newPaxosError(int32(paxos_GetInstanceValue_Value_NotExist), "get instance value, value not exists")
	errValueNotChosenYet               = newPaxosError(int32(paxos_GetInstanceValue_Value_Not_Chosen_Yet), "get instance value, value not be chosen yet")
)

var RetErrMap map[int32]error = map[int32]error{
	int32(paxos_SystemError):                           errPaxosSystemError,
	int32(paxos_GroupIdxWrong):                         errGroupIdxWrong,
	int32(paxos_MembershipOp_GidNotSame):               errMembershipOpGidNotSame,
	int32(paxos_MembershipOp_VersionConflict):           errMembershipOpVersionConflict,
	int32(paxos_MembershipOp_NoGid):                    errMembershipOpNoGid,
	int32(paxos_MembershipOp_Add_NodeExist):            errMembershipOpNodeExists,
	int32(paxos_MembershipOp_Remove_NodeNotExist):      errMembershipOpRemoveNodeNotExists,
	int32(paxos_MembershipOp_Change_NoChange):          errMembershipOpNoChange,
	int32(paxos_GetInstanceValue_Value_NotExist):       errValueNotExists,
	int32(paxos_GetInstanceValue_Value_Not_Chosen_Yet): errValueNotChosenYet,
}
