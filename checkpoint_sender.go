package gopaxos

import (
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	checkpointAckTimeout = 120000
	checkpointAckLead    = 10
)

type checkpointSender struct {
	sendNodeID uint64
	conf       *config
	learner    *learner
	smFac      *smFac
	cpMgr      *checkpointMgr

	isEnd     bool
	isEnded   bool
	isStarted bool

	uuid           uint64
	sequence       uint64
	ackSequence    uint64
	absLastAckTime uint64

	tmpBuffer         []byte
	alreadySendedFile map[string]bool

	quit chan struct{}
}

func newCheckpointSender(sendNodeID uint64, conf *config, l *learner, smFac *smFac, cpMgr *checkpointMgr) *checkpointSender {
	ret := &checkpointSender{}
	ret.sendNodeID = sendNodeID
	ret.conf = conf
	ret.learner = l
	ret.smFac = smFac
	ret.cpMgr = cpMgr
	ret.uuid = (conf.getMyNodeID() ^ l.getInstanceID()) + uint64(rand.Uint32())
	ret.tmpBuffer = make([]byte, 1048576)
	return ret
}

func (c *checkpointSender) start() {
	c.quit = make(chan struct{}, 1)
	go c.run()
}

func (c *checkpointSender) stop() {
	if c.isStarted && !c.isEnded {
		c.isEnd = true
		if c.quit != nil {
			<-c.quit
			c.quit = nil
		}
	}
}

func (c *checkpointSender) end() {
	c.isEnd = true
}

func (c *checkpointSender) hasBeenEnded() bool {
	return c.isEnded
}

func (c *checkpointSender) run() {
	c.isStarted = true
	c.absLastAckTime = getSteadyClockMS()

	//pause checkpoint rePlayer
	needContinue := false
	for !c.cpMgr.getRePlayer().isPaused() {
		if c.isEnd {
			c.isEnded = true
			close(c.quit)
			return
		}

		needContinue = true

		c.cpMgr.getRePlayer().pause()
		lPLGDebug(c.conf.groupIdx, "wait replayer paused.")
		time.Sleep(time.Millisecond * 100)
	}
	if err := c.lockCheckpoint(); err == nil {
		c.sendCheckpoint()
		c.unLockCheckpoint()
	}

	//continue checkpoint rePlayer
	if needContinue {
		c.cpMgr.getRePlayer().resume()
	}

	lPLGHead(c.conf.groupIdx, "Checkpoint.Sender [END]")

	c.isEnded = true
}

func (c *checkpointSender) lockCheckpoint() error {
	smList := c.smFac.getSMList()
	lockSMList := make([]StateMachine, 0, len(smList))

	var err error
	for _, sm := range smList {
		if err = sm.LockCheckpointState(); err != nil {
			break
		}
		lockSMList = append(lockSMList, sm)
	}

	if err != nil {
		for _, sm := range lockSMList {
			sm.UnLockCheckpointState()
		}
	}

	return err
}

func (c *checkpointSender) unLockCheckpoint() {
	for _, sm := range c.smFac.getSMList() {
		sm.UnLockCheckpointState()
	}
}

func (c *checkpointSender) sendCheckpoint() {
	err := c.learner.sendCheckpointBegin(c.sendNodeID, c.uuid, c.sequence, c.smFac.getCheckpointInstanceID(c.conf.groupIdx))
	if err != nil {
		lPLGErr(c.conf.groupIdx, "SendCheckpointBegin fail, error: %v", err)
		return
	}

	err = c.learner.sendCheckpointBegin(c.sendNodeID, c.uuid, c.sequence, c.smFac.getCheckpointInstanceID(c.conf.groupIdx))
	if err != nil {
		lPLGErr(c.conf.groupIdx, "SendCheckpointBegin fail, error: %v", err)
		return
	}

	getBPInstance().SendCheckpointBegin()

	c.sequence++

	for _, sm := range c.smFac.getSMList() {
		if err = c.sendCheckpointOfASM(sm); err != nil {
			return
		}
	}

	err = c.learner.sendCheckpointEnd(c.sendNodeID, c.uuid, c.sequence, c.smFac.getCheckpointInstanceID(c.conf.groupIdx))
	if err != nil {
		lPLGErr(c.conf.groupIdx, "SendCheckpointEnd fail, sequence %d, error: %v", c.sequence, err)
	}

	getBPInstance().SendCheckpointEnd()
}

func (c *checkpointSender) sendCheckpointOfASM(sm StateMachine) error {
	dirPath, fileList, err := sm.GetCheckpointState(c.conf.getMyGroupIdx())
	if err != nil {
		lPLGErr(c.conf.groupIdx, "GetCheckpointState fail, smid %d, error: %v", sm.SMID(), err)
		return err
	}

	if dirPath == "" {
		lPLGImp(c.conf.groupIdx, "No Checkpoint, smid %d", sm.SMID())
		return nil
	}

	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	for _, filePath := range fileList {
		if err = c.sendFile(sm, dirPath, filePath); err != nil {
			lPLGErr(c.conf.groupIdx, "SendFile fail, smid %d, error: %v", sm.SMID(), err)
			return err
		}
	}

	lPLGImp(c.conf.groupIdx, "END, send ok, smid %d filelistcount %d", sm.SMID(), len(fileList))

	return nil
}

func (c *checkpointSender) sendFile(sm StateMachine, dirPath, filePath string) error {
	lPLGHead(c.conf.groupIdx, "START smid %d dirpath %s filepath %s", sm.SMID(), dirPath, filePath)

	path := dirPath + filePath

	if _, ok := c.alreadySendedFile[path]; ok {
		lPLGErr(c.conf.groupIdx, "file already send, filepath %s", path)
		return nil
	}

	fd, err := os.OpenFile(path, os.O_RDWR, 0400)
	if err != nil {
		lPLGErr(c.conf.groupIdx, "Open file fail, filepath %s", path)
		return err
	}
	defer fd.Close()
	var readLen, offset int

	for {
		readLen, err = fd.Read(c.tmpBuffer)
		// also err == io.EOF
		if readLen == 0 {
			break
		}

		if err != nil {
			return err
		}

		if err = c.sendBuffer(sm.SMID(), sm.GetCheckpointInstanceID(c.conf.groupIdx), filePath, uint64(offset), c.tmpBuffer[:readLen]); err != nil {
			return err
		}

		lPLGDebug(c.conf.groupIdx, "Send ok, offset %d readlen %d", offset, readLen)

		if readLen < len(c.tmpBuffer) {
			break
		}

		offset += readLen
	}

	c.alreadySendedFile[path] = true

	lPLGImp(c.conf.groupIdx, "END")

	return nil
}

func (c *checkpointSender) sendBuffer(smID int64, cpInstanceID uint64, filePath string, offset uint64, buffer []byte) error {
	checksum := crc(0, buffer)

	for {
		if c.isEnd {
			return errCheckpointSenderEnded
		}

		if !c.checkAck(c.sequence) {
			return errCheckpointAck
		}

		err := c.learner.sendCheckpoint(c.sendNodeID, c.uuid, c.sequence, cpInstanceID, checksum, filePath, smID, offset, buffer)

		getBPInstance().SendCheckpointOneBlock()

		if err == nil {
			c.sequence++
			return err
		} else {
			lPLGErr(c.conf.groupIdx, "SendCheckpoint fail, error: %d need sleep 30s", err)
			time.Sleep(time.Millisecond * 30000)
		}
	}

	return nil
}

func (c *checkpointSender) ack(sendNodeID uint64, uuid uint64, sequence uint64) {
	if sendNodeID != c.sendNodeID {
		lPLGErr(c.conf.groupIdx, "send nodeid not same, ack.sendnodeid %d self.sendnodeid %d", sendNodeID, c.sendNodeID)
		return
	}

	if uuid != c.uuid {
		lPLGErr(c.conf.groupIdx, "uuid not same, ack.uuid %d self.uuid %d", uuid, c.uuid)
		return
	}

	if sequence != c.ackSequence {
		lPLGErr(c.conf.groupIdx, "ack_sequence not same, ack.ack_sequence %d self.ack_sequence %d",
			sequence, c.ackSequence)
		return
	}

	c.ackSequence++
	c.absLastAckTime = getSteadyClockMS()
}

func (c *checkpointSender) checkAck(sendSequence uint64) bool {
	for sendSequence > c.ackSequence+checkpointAckLead {
		now := getSteadyClockMS()
		var passTime uint64
		if now > c.absLastAckTime {
			passTime = now - c.absLastAckTime
		}

		if c.isEnd {
			return false
		}

		if passTime >= checkpointAckTimeout {
			lPLGErr(c.conf.groupIdx, "Ack timeout, last acktime %d", c.absLastAckTime)
			return false
		}

		time.Sleep(time.Millisecond * 20)
	}

	return true
}
