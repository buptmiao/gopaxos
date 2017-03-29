package gopaxos

import (
	"unsafe"
)

const (
	system_V_SMID int64 = 100000000 + iota
	master_V_SMID
	batch_Propose_SMID
)

type paxosTryCommitRet int32

const (
	paxosTryCommitRet_Ok                          paxosTryCommitRet = 0
	paxosTryCommitRet_Reject                      paxosTryCommitRet = -2
	paxosTryCommitRet_Conflict                    paxosTryCommitRet = 14
	paxosTryCommitRet_ExecuteFail                 paxosTryCommitRet = 15
	paxosTryCommitRet_Follower_Cannot_Commit      paxosTryCommitRet = 16
	paxosTryCommitRet_IM_Not_In_Membership        paxosTryCommitRet = 17
	paxosTryCommitRet_Value_Size_TooLarge         paxosTryCommitRet = 18
	paxosTryCommitRet_Timeout                     paxosTryCommitRet = 404
	paxosTryCommitRet_TooManyThreadWaiting_Reject paxosTryCommitRet = 405
)

type paxosNodeFunctionRet int32

const (
	paxos_SystemError                           paxosNodeFunctionRet = -1
	paxos_GroupIdxWrong                         paxosNodeFunctionRet = -5
	paxos_MembershipOp_GidNotSame               paxosNodeFunctionRet = -501
	paxos_MembershipOp_VersionConflict 			paxosNodeFunctionRet = -502
	paxos_MembershipOp_NoGid                    paxosNodeFunctionRet = 1001
	paxos_MembershipOp_Add_NodeExist            paxosNodeFunctionRet = 1002
	paxos_MembershipOp_Remove_NodeNotExist      paxosNodeFunctionRet = 1003
	paxos_MembershipOp_Change_NoChange          paxosNodeFunctionRet = 1004
	paxos_GetInstanceValue_Value_NotExist       paxosNodeFunctionRet = 1005
	paxos_GetInstanceValue_Value_Not_Chosen_Yet paxosNodeFunctionRet = 1006
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
	max_Queue_Mem_Size = 209715200
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
	msgType_PaxosLearner_ConfirmAskforLearn
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
	timer_Learner_AskForLearn_Noop
	timer_Instance_Commit_Timeout
)
