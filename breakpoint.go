package gopaxos

type Breakpoint interface {
	ProposerBP
	AcceptorBP
	LearnerBP
	InstanceBP
	CommitterBP
	IOLoopBP
	NetworkBP
	LogStorageBP
	AlgorithmBaseBP
	CheckpointBP
	MasterBP
}

type ProposerBP interface {
	NewProposal(value []byte)
	NewProposalSkipPrepare()
	Prepare()
	OnPrepareReply()
	OnPrepareReplyButNotPreparing()
	OnPrepareReplyNotSameProposalIDMsg()
	PreparePass(useTimeMs int)
	PrepareNotPass()
	Accept()
	OnAcceptReply()
	OnAcceptReplyButNotAccepting()
	OnAcceptReplyNotSameProposalIDMsg()
	AcceptPass(useTimeMs int)
	AcceptNotPass()
	PrepareTimeout()
	AcceptTimeout()
}

type AcceptorBP interface {
	OnPrepare()
	OnPreparePass()
	OnPreparePersistFail()
	OnPrepareReject()
	OnAccept()
	OnAcceptPass()
	OnAcceptPersistFail()
	OnAcceptReject()
}

type LearnerBP interface {
	AskForLearn()
	OnAskForLearn()
	OnAskForLearnGetLockFail()
	SendNowInstanceID()
	OnSendNowInstanceID()
	ConfirmAskForLearn()
	OnConfirmAskForLearn()
	OnConfirmAskForLearnGetLockFail()
	SendLearnValue()
	OnSendLearnValue()
	SendLearnValue_Ack()
	OnSendLearnValue_Ack()
	ProposerSendSuccess()
	OnProposerSendSuccess()
	OnProposerSendSuccessNotAcceptYet()
	OnProposerSendSuccessBallotNotSame()
	OnProposerSendSuccessSuccessLearn()
	SenderAckTimeout()
	SenderAckDelay()
	SenderSendOnePaxosLog()
}

type InstanceBP interface {
	NewInstance()
	SendMessage()
	BroadcastMessage()
	OnNewValueCommitTimeout()
	OnReceive()
	OnReceiveParseError()
	OnReceivePaxosMsg()
	OnReceivePaxosMsgNodeIDNotValid()
	OnReceivePaxosMsgTypeNotValid()
	OnReceivePaxosProposerMsgINotSame()
	OnReceivePaxosAcceptorMsgINotSame()
	OnReceivePaxosAcceptorMsgAddRetry()
	OnInstanceLearned()
	OnInstanceLearnedNotMyCommit()
	OnInstanceLearnedIsMyCommit(useTimeMs int)
	OnInstanceLearnedSMExecuteFail()
	ChecksumLogicFail()
}

type CommitterBP interface {
	NewValue()
	NewValueConflict()
	NewValueGetLockTimeout()
	NewValueGetLockReject()
	NewValueGetLockOK(useTimeMs int)
	NewValueCommitOK(useTimeMs int)
	NewValueCommitFail()
	BatchPropose()
	BatchProposeOK()
	BatchProposeFail()
	BatchProposeWaitTimeMs(waitTimeMs int)
	BatchProposeDoPropose(batchCount int)
}

type IOLoopBP interface {
	OneLoop()
	EnqueueMsg()
	EnqueueMsgRejectByFullQueue()
	EnqueueRetryMsg()
	EnqueueRetryMsgRejectByFullQueue()
	OutQueueMsg()
	DealWithRetryMsg()
}

type NetworkBP interface {
	TcpEpollLoop()
	TcpOnError()
	TcpAcceptFd()
	TcpQueueFull()
	TcpReadOneMessageOk(len int)
	TcpOnReadMessageLenError()
	TcpReconnect()
	TcpOutQueue(delayMs int)
	SendRejectByTooLargeSize()
	Send(message string)
	SendTcp(message string)
	SendUdp(message string)
	SendMessageNodeIDNotFound()
	UDPReceive(recvLen int)
	UDPRealSend(message string)
	UDPQueueFull()
}

type LogStorageBP interface {
	LevelDBGetNotExist()
	LevelDBGetFail()
	FileIDToValueFail()
	ValueToFileIDFail()
	LevelDBPutFail()
	LevelDBPutOK(useTimeMs int)
	AppendDataFail()
	AppendDataOK(writeLen, useTimeMs int)
	GetFileChecksumNotEqual()
}

type AlgorithmBaseBP interface {
	UnPackHeaderLenTooLong()
	UnPackChecksumNotSame()
	HeaderGidNotSame()
}

type CheckpointBP interface {
	NeedAskForCheckpoint()
	SendCheckpointOneBlock()
	OnSendCheckpointOneBlock()
	SendCheckpointBegin()
	SendCheckpointEnd()
	ReceiveCheckpointDone()
	ReceiveCheckpointAndLoadFail()
	ReceiveCheckpointAndLoadSucc()
}

type MasterBP interface {
	TryBeMaster()
	TryBeMasterProposeFail()
	SuccessBeMaster()
	OtherBeMaster()
	DropMaster()
	MasterSMInconsistent()
}

var bpInstance Breakpoint

func getBPInstance() Breakpoint {
	if bpInstance != nil {
		return bpInstance
	}
	return &dfBreakpoint{}
}

func setBPInstance(bp Breakpoint) {
	bpInstance = bp
}

type dfBreakpoint bool

//ProposerBP
func (d *dfBreakpoint) NewProposal(value string)            {}
func (d *dfBreakpoint) NewProposalSkipPrepare()             {}
func (d *dfBreakpoint) Prepare()                            {}
func (d *dfBreakpoint) OnPrepareReply()                     {}
func (d *dfBreakpoint) OnPrepareReplyButNotPreparing()      {}
func (d *dfBreakpoint) OnPrepareReplyNotSameProposalIDMsg() {}
func (d *dfBreakpoint) PreparePass(useTimeMs int)           {}
func (d *dfBreakpoint) PrepareNotPass()                     {}
func (d *dfBreakpoint) Accept()                             {}
func (d *dfBreakpoint) OnAcceptReply()                      {}
func (d *dfBreakpoint) OnAcceptReplyButNotAccepting()       {}
func (d *dfBreakpoint) OnAcceptReplyNotSameProposalIDMsg()  {}
func (d *dfBreakpoint) AcceptPass(useTimeMs int)            {}
func (d *dfBreakpoint) AcceptNotPass()                      {}
func (d *dfBreakpoint) PrepareTimeout()                     {}
func (d *dfBreakpoint) AcceptTimeout()                      {}

//AcceptorBP
func (d *dfBreakpoint) OnPrepare()            {}
func (d *dfBreakpoint) OnPreparePass()        {}
func (d *dfBreakpoint) OnPreparePersistFail() {}
func (d *dfBreakpoint) OnPrepareReject()      {}
func (d *dfBreakpoint) OnAccept()             {}
func (d *dfBreakpoint) OnAcceptPass()         {}
func (d *dfBreakpoint) OnAcceptPersistFail()  {}
func (d *dfBreakpoint) OnAcceptReject()       {}

//LearnerBP
func (d *dfBreakpoint) AskForLearn()                        {}
func (d *dfBreakpoint) OnAskForLearn()                      {}
func (d *dfBreakpoint) OnAskForLearnGetLockFail()           {}
func (d *dfBreakpoint) SendNowInstanceID()                  {}
func (d *dfBreakpoint) OnSendNowInstanceID()                {}
func (d *dfBreakpoint) ConfirmAskForLearn()                 {}
func (d *dfBreakpoint) OnConfirmAskForLearn()               {}
func (d *dfBreakpoint) OnConfirmAskForLearnGetLockFail()    {}
func (d *dfBreakpoint) SendLearnValue()                     {}
func (d *dfBreakpoint) OnSendLearnValue()                   {}
func (d *dfBreakpoint) SendLearnValue_Ack()                 {}
func (d *dfBreakpoint) OnSendLearnValue_Ack()               {}
func (d *dfBreakpoint) ProposerSendSuccess()                {}
func (d *dfBreakpoint) OnProposerSendSuccess()              {}
func (d *dfBreakpoint) OnProposerSendSuccessNotAcceptYet()  {}
func (d *dfBreakpoint) OnProposerSendSuccessBallotNotSame() {}
func (d *dfBreakpoint) OnProposerSendSuccessSuccessLearn()  {}
func (d *dfBreakpoint) SenderAckTimeout()                   {}
func (d *dfBreakpoint) SenderAckDelay()                     {}
func (d *dfBreakpoint) SenderSendOnePaxosLog()              {}

//InstanceBP
func (d *dfBreakpoint) NewInstance()                              {}
func (d *dfBreakpoint) SendMessage()                              {}
func (d *dfBreakpoint) BroadcastMessage()                         {}
func (d *dfBreakpoint) OnNewValueCommitTimeout()                  {}
func (d *dfBreakpoint) OnReceive()                                {}
func (d *dfBreakpoint) OnReceiveParseError()                      {}
func (d *dfBreakpoint) OnReceivePaxosMsg()                        {}
func (d *dfBreakpoint) OnReceivePaxosMsgNodeIDNotValid()          {}
func (d *dfBreakpoint) OnReceivePaxosMsgTypeNotValid()            {}
func (d *dfBreakpoint) OnReceivePaxosProposerMsgINotSame()        {}
func (d *dfBreakpoint) OnReceivePaxosAcceptorMsgINotSame()        {}
func (d *dfBreakpoint) OnReceivePaxosAcceptorMsgAddRetry()        {}
func (d *dfBreakpoint) OnInstanceLearned()                        {}
func (d *dfBreakpoint) OnInstanceLearnedNotMyCommit()             {}
func (d *dfBreakpoint) OnInstanceLearnedIsMyCommit(useTimeMs int) {}
func (d *dfBreakpoint) OnInstanceLearnedSMExecuteFail()           {}
func (d *dfBreakpoint) ChecksumLogicFail()                        {}

//CommitterBP
func (d *dfBreakpoint) NewValue()                             {}
func (d *dfBreakpoint) NewValueConflict()                     {}
func (d *dfBreakpoint) NewValueGetLockTimeout()               {}
func (d *dfBreakpoint) NewValueGetLockReject()                {}
func (d *dfBreakpoint) NewValueGetLockOK(useTimeMs int)       {}
func (d *dfBreakpoint) NewValueCommitOK(useTimeMs int)        {}
func (d *dfBreakpoint) NewValueCommitFail()                   {}
func (d *dfBreakpoint) BatchPropose()                         {}
func (d *dfBreakpoint) BatchProposeOK()                       {}
func (d *dfBreakpoint) BatchProposeFail()                     {}
func (d *dfBreakpoint) BatchProposeWaitTimeMs(waitTimeMs int) {}
func (d *dfBreakpoint) BatchProposeDoPropose(batchCount int)  {}

//IOLoopBP
func (d *dfBreakpoint) OneLoop()                          {}
func (d *dfBreakpoint) EnqueueMsg()                       {}
func (d *dfBreakpoint) EnqueueMsgRejectByFullQueue()      {}
func (d *dfBreakpoint) EnqueueRetryMsg()                  {}
func (d *dfBreakpoint) EnqueueRetryMsgRejectByFullQueue() {}
func (d *dfBreakpoint) OutQueueMsg()                      {}
func (d *dfBreakpoint) DealWithRetryMsg()                 {}

//NetworkBP
func (d *dfBreakpoint) TcpEpollLoop()               {}
func (d *dfBreakpoint) TcpOnError()                 {}
func (d *dfBreakpoint) TcpAcceptFd()                {}
func (d *dfBreakpoint) TcpQueueFull()               {}
func (d *dfBreakpoint) TcpReadOneMessageOk(len int) {}
func (d *dfBreakpoint) TcpOnReadMessageLenError()   {}
func (d *dfBreakpoint) TcpReconnect()               {}
func (d *dfBreakpoint) TcpOutQueue(delayMs int)     {}
func (d *dfBreakpoint) SendRejectByTooLargeSize()   {}
func (d *dfBreakpoint) Send(message string)         {}
func (d *dfBreakpoint) SendTcp(message string)      {}
func (d *dfBreakpoint) SendUdp(message string)      {}
func (d *dfBreakpoint) SendMessageNodeIDNotFound()  {}
func (d *dfBreakpoint) UDPReceive(recvLen int)      {}
func (d *dfBreakpoint) UDPRealSend(message string)  {}
func (d *dfBreakpoint) UDPQueueFull()               {}

//LogStorageBP
func (d *dfBreakpoint) LevelDBGetNotExist()                  {}
func (d *dfBreakpoint) LevelDBGetFail()                      {}
func (d *dfBreakpoint) FileIDToValueFail()                   {}
func (d *dfBreakpoint) ValueToFileIDFail()                   {}
func (d *dfBreakpoint) LevelDBPutFail()                      {}
func (d *dfBreakpoint) LevelDBPutOK(useTimeMs int)           {}
func (d *dfBreakpoint) AppendDataFail()                      {}
func (d *dfBreakpoint) AppendDataOK(writeLen, useTimeMs int) {}
func (d *dfBreakpoint) GetFileChecksumNotEqual()             {}

//AlgorithmBaseBP
func (d *dfBreakpoint) UnPackHeaderLenTooLong() {}
func (d *dfBreakpoint) UnPackChecksumNotSame()  {}
func (d *dfBreakpoint) HeaderGidNotSame()       {}

//CheckpointBP
func (d *dfBreakpoint) NeedAskForCheckpoint()         {}
func (d *dfBreakpoint) SendCheckpointOneBlock()       {}
func (d *dfBreakpoint) OnSendCheckpointOneBlock()     {}
func (d *dfBreakpoint) SendCheckpointBegin()          {}
func (d *dfBreakpoint) SendCheckpointEnd()            {}
func (d *dfBreakpoint) ReceiveCheckpointDone()        {}
func (d *dfBreakpoint) ReceiveCheckpointAndLoadFail() {}
func (d *dfBreakpoint) ReceiveCheckpointAndLoadSucc() {}

//MasterBP
func (d *dfBreakpoint) TryBeMaster()            {}
func (d *dfBreakpoint) TryBeMasterProposeFail() {}
func (d *dfBreakpoint) SuccessBeMaster()        {}
func (d *dfBreakpoint) OtherBeMaster()          {}
func (d *dfBreakpoint) DropMaster()             {}
func (d *dfBreakpoint) MasterSMInconsistent()   {}
