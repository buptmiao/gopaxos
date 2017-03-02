package gopaxos

import "time"

type rePlayer struct {
	conf     *config
	smFac    *smFac
	paxosLog *paxosLog
	cpMgr    *checkpointMgr

	canRun  bool
	isPause bool
	isEnd   bool
	quit    chan struct{}
}

func newRePlayer(conf *config, smFac *smFac, ls LogStorage, cpMgr *checkpointMgr) *rePlayer {
	return &rePlayer{
		conf:     conf,
		smFac:    smFac,
		paxosLog: newPaxosLog(ls),
		cpMgr:    cpMgr,
		isPause:  true,
	}
}

func (r *rePlayer) start() {
	r.quit = make(chan struct{}, 1)
	go r.run()
}

func (r *rePlayer) stop() {
	r.isEnd = true
	if r.quit != nil {
		<-r.quit
		r.quit = nil
	}
}

func (r *rePlayer) pause() {
	r.canRun = false
}

func (r *rePlayer) resume() {
	r.isPause = false
	r.canRun = true
}

func (r *rePlayer) isPaused() bool {
	return r.isPause
}

func (r *rePlayer) run() {
	lPLGHead(r.conf.groupIdx, "Checkpoint.Replayer [START]")
	instanceID := r.smFac.getCheckpointInstanceID(r.conf.groupIdx) + 1

	for {
		if r.isEnd {
			lPLGHead(r.conf.groupIdx, "Checkpoint.Replayer [END]")
			close(r.quit)
			return
		}

		if !r.canRun {
			r.isPause = true
			time.Sleep(time.Second)
			continue
		}

		if instanceID >= r.cpMgr.getMaxChosenInstanceID() {
			time.Sleep(time.Second)
			continue
		}

		if r.playOne(instanceID) {
			lPLGImp(r.conf.groupIdx, "Play one done, instanceid %d", instanceID)
			instanceID++
		} else {
			lPLGErr(r.conf.groupIdx, "Play one fail, instanceid %d", instanceID)
			time.Sleep(time.Millisecond * 500)
		}
	}
}

func (r *rePlayer) playOne(instanceID uint64) bool {
	state, err := r.paxosLog.readState(r.conf.groupIdx, instanceID)
	if err != nil {
		return false
	}

	if !r.smFac.executeForCheckpoint(r.conf.groupIdx, instanceID, state.GetAcceptedValue()) {
		lPLGErr("Checkpoint sm excute fail, instanceid %d", instanceID)
		return false
	}

	return true
}
