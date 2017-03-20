package gopaxos

import (
	"math"
	"time"
)

type learnerSender struct {
	conf            *config
	learner         *learner
	paxosLog        *paxosLog
	slock           *serialLock
	isSending       bool
	absLastSendTime uint64
	beginInstanceID uint64
	sendToNodeID    uint64
	isConfirmed     bool
	ackInstanceID   uint64
	absLastAckTime  uint64
	ackLead         int
	isEnd           bool
	isStart         bool
	quit            chan struct{}
}

func newLearnerSender(conf *config, learner *learner, paxosLog *paxosLog) *learnerSender {
	ret := &learnerSender{
		conf:     conf,
		learner:  learner,
		paxosLog: paxosLog,
		ackLead:  getInsideOptionsInstance().getLearnerSenderAckLead(),
		slock:    newSerialLock(),
	}

	ret.sendDone()

	return ret
}

func (l *learnerSender) start() {
	l.quit = make(chan struct{}, 1)
	go l.run()

}

func (l *learnerSender) run() {
	l.isStart = true

	for {
		l.waitToSend()

		if l.isEnd {
			lPLGHead(l.conf.groupIdx, "Learner.Sender [END]")
			close(l.quit)
			return
		}

		l.sendLearnedValue(l.beginInstanceID, l.sendToNodeID)

		l.sendDone()
	}
}

func (l *learnerSender) stop() {
	if l.isStart {
		l.isEnd = true
		if l.quit != nil {
			<-l.quit
			l.quit = nil
		}
	}
}

func (l *learnerSender) refreshSending() {
	l.absLastSendTime = getSteadyClockMS()
}

func (l *learnerSender) isIMSending() bool {
	if !l.isSending {
		return false
	}

	now := getSteadyClockMS()
	var passTime uint64
	if now > l.absLastSendTime {
		passTime = now - l.absLastSendTime
	}

	if passTime >= uint64(getInsideOptionsInstance().getLearnerSenderPrepareTimeoutMs()) {
		return false
	}

	return true
}

func (l *learnerSender) cutAckLead() {
	receiveAckLead := getInsideOptionsInstance().getLearnerReceiverAckLead()
	if l.ackLead-receiveAckLead > receiveAckLead {
		l.ackLead = l.ackLead - receiveAckLead
	}
}

func (l *learnerSender) checkAck(instanceID uint64) bool {
	l.slock.lock()
	defer l.slock.unlock()
	if instanceID < l.ackInstanceID {
		l.ackLead = getInsideOptionsInstance().getLearnerSenderAckLead()
		lPLGImp(l.conf.groupIdx, "Already catch up, ack instanceid %d now send instanceid %d",
			l.ackInstanceID, instanceID)
		return false
	}

	for instanceID > l.ackInstanceID+uint64(l.ackLead) {
		now := getSteadyClockMS()
		var passTime uint64
		if now > l.absLastAckTime {
			passTime = now - l.absLastAckTime
		}

		if passTime >= uint64(getInsideOptionsInstance().getLearnerSenderAckTimeoutMs()) {
			getBPInstance().SenderAckTimeout()
			lPLGErr(l.conf.groupIdx, "Ack timeout, last acktime %d now send instanceid %d",
				l.absLastAckTime, instanceID)
			l.cutAckLead()
			return false
		}

		getBPInstance().SenderAckDelay()

		l.slock.waitTime(time.Millisecond * 20)
	}

	return true
}

func (l *learnerSender) prepare(beginInstanceID uint64, sendToNodeID uint64) bool {
	l.slock.lock()
	defer l.slock.unlock()

	if !l.isIMSending() && !l.isConfirmed {
		l.isSending = true
		l.absLastAckTime = getSteadyClockMS()
		l.absLastSendTime = l.absLastAckTime
		l.ackInstanceID = beginInstanceID
		l.beginInstanceID = beginInstanceID
		l.sendToNodeID = sendToNodeID

		return true
	}

	return false
}

func (l *learnerSender) confirm(beginInstanceID uint64, sendToNodeID uint64) bool {
	l.slock.lock()
	defer l.slock.unlock()

	if l.isIMSending() && !l.isConfirmed {
		if l.beginInstanceID == beginInstanceID && l.sendToNodeID == sendToNodeID {
			l.isConfirmed = true
			l.slock.interrupt()

			return true
		}
	}

	return false
}

func (l *learnerSender) ack(ackInstanceID uint64, fromNodeID uint64) {
	l.slock.lock()
	defer l.slock.unlock()

	if l.isIMSending() && l.isConfirmed {
		if l.sendToNodeID == fromNodeID && ackInstanceID > l.ackInstanceID {
			l.ackInstanceID = ackInstanceID
			l.absLastAckTime = getSteadyClockMS()
			l.slock.interrupt()
		}
	}
}

func (l *learnerSender) waitToSend() {
	l.slock.lock()
	defer l.slock.unlock()

	for !l.isConfirmed {
		l.slock.waitTime(time.Millisecond * 1000)
		if l.isEnd {
			break
		}
	}
}

func (l *learnerSender) sendLearnedValue(beginInstanceID uint64, sendToNodeID uint64) {
	lPLGHead(l.conf.groupIdx, "BeginInstanceID %d SendToNodeID %d", beginInstanceID, sendToNodeID)
	sendInstanceID := beginInstanceID

	sendQps := getInsideOptionsInstance().getLearnerSenderSendQps()
	sleepMs := 1000 / sendQps
	sendInterval := 1
	if sendQps > 1000 {
		sleepMs = 1
		sendInterval = sendQps / 1000
	}

	lPLGDebug(l.conf.groupIdx, "SendQps %d SleepMs %d SendInterval %d AckLead %d",
		sendQps, sleepMs, sendInterval, l.ackLead)

	var lastChecksum uint32
	sendCount := 0

	for sendInstanceID < l.learner.getInstanceID() {
		err := l.sendOne(sendInstanceID, sendToNodeID, &lastChecksum)
		if err != nil {
			lPLGErr(l.conf.groupIdx, "SendOne fail, SendInstanceID %d SendToNodeID %d, error: %v",
				sendInstanceID, sendToNodeID, err)
			return
		}

		if !l.checkAck(sendInstanceID) {
			return
		}

		sendCount++
		sendInstanceID++
		l.refreshSending()

		if sendCount >= sendInterval {
			sendCount = 0
			time.Sleep(time.Millisecond * time.Duration(sleepMs))
		}
	}

	//succ send, reset ack lead.
	l.ackLead = getInsideOptionsInstance().getLearnerSenderAckLead()
	lPLGImp(l.conf.groupIdx, "SendDone, SendEndInstanceID %d", sendInstanceID)
}

func (l *learnerSender) sendOne(sendInstanceID uint64, sendToNodeID uint64, lastChecksum *uint32) error {
	getBPInstance().SenderSendOnePaxosLog()
	state, err := l.paxosLog.readState(l.conf.groupIdx, sendInstanceID)
	if err != nil {
		return err
	}

	ballot := newBallotNumber(state.GetAcceptedID(), state.GetAcceptedNodeID())

	err = l.learner.sendLearnValue(sendToNodeID, sendInstanceID, ballot, state.GetAcceptedValue(), *lastChecksum, true)

	*lastChecksum = state.GetChecksum()

	return err
}

func (l *learnerSender) sendDone() {
	l.slock.lock()
	defer l.slock.unlock()

	l.isSending = false
	l.isConfirmed = false
	l.beginInstanceID = math.MaxUint64
	l.sendToNodeID = nullNode
	l.absLastSendTime = 0
	l.ackInstanceID = 0
	l.absLastAckTime = 0
}
