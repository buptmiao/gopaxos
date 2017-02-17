package paxos

type instance struct {
	conf  *config
	comm  MsgTransport
	smFac *smFac
}

func newInstance(conf *config, ls LogStorage, tran MsgTransport, opt *Options) *instance {

}

func (i *instance) init() error {

}

func (i *instance) start() {

}

func (i *instance) getCommitter() *committer {

}

func (i *instance) getCheckpointCleaner() *cleaner {

}

func (i *instance) getCheckpointRePlayer() *rePlayer {

}

func (i *instance) addStateMachine(sm StateMachine) {

}
