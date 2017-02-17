package paxos

import (
	"github.com/pkg/errors"
	"unsafe"
)

const (
	system_v_smid int = 100000000 + iota
	master_v_smid
	batch_propose_smid
)

var ()

type paxostrycommitret int32

const (
	paxostrycommitret_ok                          paxostrycommitret = 0
	paxostrycommitret_reject                      paxostrycommitret = -2
	paxostrycommitret_conflict                    paxostrycommitret = 14
	paxostrycommitret_executefail                 paxostrycommitret = 15
	paxostrycommitret_follower_cannot_commit      paxostrycommitret = 16
	paxostrycommitret_im_not_in_membership        paxostrycommitret = 17
	paxostrycommitret_value_size_toolarge         paxostrycommitret = 18
	paxostrycommitret_timeout                     paxostrycommitret = 404
	paxostrycommitret_toomanythreadwaiting_reject paxostrycommitret = 405
)

type paxosnodefunctionret int32

const (
	paxos_systemerror                           paxosnodefunctionret = -1
	paxos_groupidxwrong                         paxosnodefunctionret = -5
	paxos_membershipop_gidnotsame               paxosnodefunctionret = -501
	paxos_membershipop_versionconflit           paxosnodefunctionret = -502
	paxos_membershipop_nogid                    paxosnodefunctionret = 1001
	paxos_membershipop_add_nodeexist            paxosnodefunctionret = 1002
	paxos_membershipop_remove_nodenotexist      paxosnodefunctionret = 1003
	paxos_membershipop_change_nochange          paxosnodefunctionret = 1004
	paxos_getinstancevalue_value_notexist       paxosnodefunctionret = 1005
	paxos_getinstancevalue_value_not_chosen_yet paxosnodefunctionret = 1006
)

const (
	nullvalue     = "nullvalue"
	intlen        = int(unsafe.Sizeof(int(0)))
	crc32skip     = 8
	net_crc32skip = 7

	//network protocal
	groupidxlen  = intlen
	headlen_len  = 2
	checksum_len = 4

	//max queue memsize
	max_queue_mem_size = 209715200
)

type msgCmd int32

const (
	msgCmd_PaxosMsg msgCmd = 1 + iota
	msgCmd_CheckpointMsg
)

type paxosMsgType int32

const (
	msgType_PaxosPrepare paxosMsgType = 1 + iota
	msgType_PaxosPrepareReply
	msgType_PaxosAccept
	msgType_PaxosAcceptReply
	msgType_PaxosLearner_AskforLearn
	msgType_PaxosLearner_SendLearnValue
	msgType_PaxosLearner_ProposerSendSuccess
	msgType_PaxosProposal_SendNewValue
	msgType_PaxosLearner_SendNowInstanceID
	msgType_PaxosLearner_ComfirmAskforLearn
	msgType_PaxosLearner_SendLearnValue_Ack
	msgType_PaxosLearner_AskforCheckpoint
	msgType_PaxosLearner_OnAskforCheckpoint
)

type paxosMsgFlagType int32

const (
	paxosMsgFlagType_SendLearnValue_NeedAck paxosMsgFlagType = 1
)

type checkpointMsgType int32

const (
	checkpointMsgType_SendFile checkpointMsgType = 1 + iota
	checkpointMsgType_SendFile_Ack
)

type checkpointSendFileFlag int32

const (
	checkpointSendFileFlag_BEGIN checkpointSendFileFlag = 1 + iota
	checkpointSendFileFlag_ING
	checkpointSendFileFlag_END
)

type checkpointSendFileAckFlag int32

const (
	checkpointSendFileAckFlag_OK checkpointSendFileAckFlag = 1 + iota
	checkpointSendFileAckFlag_Fail
)

type timerType int32

const (
	timer_Proposer_Prepare_Timeout timerType = 1 + iota
	timer_Proposer_Accept_Timeout
	timer_Learner_Askforlearn_noop
	timer_Instance_Commit_Timeout
)

var (
	ErrLogStoragePathEmpty = errors.New("empty path of log storage")
	ErrNotFoundFromStorage = errors.New("Not found from storage")
	ErrUDPMaxSizeTooLarge  = errors.New("udp max size too large")
	ErrGroupCount          = errors.New("group count too proper")
	ErrSelfNodeFollowed    = errors.New("self node follow itself")
	ErrGroupIdxOutOfRange  = errors.New("group index out of range")

	errChecksumNotMatch       = errors.New("checksum error")
	errFileBroken             = errors.New("file is broken")
	errMetaFileBroken         = errors.New("meta file broken")
	errGetFileID              = errors.New("get file id failed")
	errFileIDTooLarge         = errors.New("del fileid too large")
	errFileIDMismatch         = errors.New("fileid mismatch")
	errFileNotExist           = errors.New("file not exists")
	errFileSizeWrong          = errors.New("file size is wrong")
	errDBNotInit              = errors.New("db not init yet")
	errFileInstanceIDMismatch = errors.New("file instanceid not match")
	errInstanceIDSizeWrong    = errors.New("instance id size wrong")
	errGroupCountNotProper    = errors.New("group count wrong")

	errMaxInstanceIDNotExist = errors.New("max instance id not exists")
	errMsgQueueFull          = errors.New("msg queue is full")
	errVersionNotInit        = errors.New("version not init")
	errGidNotSame            = errors.New("gid not same")
)
