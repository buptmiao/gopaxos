package gopaxos

import (
	"container/list"
	"github.com/buptmiao/gopaxos/paxospb"
	"time"
)

type pendingProposal struct {
	value          []byte
	ctx            *SMCtx
	instanceID     *uint64
	batchIndex     *int32
	notifier       *notifier
	absEnqueueTime uint64
}

func newPendingProposal() *pendingProposal {
	return &pendingProposal{}
}

type proposeBatch struct {
	ID                uint64
	groupIdx          int
	paxosNode         Node
	pool              *notifierPool
	slock             *serialLock
	queue             *list.List
	isEnd             bool
	isStarted         bool
	nowQueueValueSize int
	batchCount        int
	batchDelayTimeMs  int
	batchMaxSize      int
	quit              chan struct{}
}

func newProposeBatch(groupIdx int, paxosNode Node, pool *notifierPool) *proposeBatch {
	ret := &proposeBatch{}
	ret.ID = getUniqueID()
	ret.groupIdx = groupIdx
	ret.paxosNode = paxosNode
	ret.pool = pool
	ret.batchCount = 5
	ret.batchDelayTimeMs = 20
	ret.batchMaxSize = 500 * 1024
	ret.slock = newSerialLock()
	ret.queue = list.New()

	return ret
}

func (p *proposeBatch) start() {
	p.quit = make(chan struct{}, 1)
	go p.run()
}

func (p *proposeBatch) run() {
	p.isStarted = true
	var ts timeStat
	for {
		p.slock.lock()

		if p.isEnd {
			p.slock.unlock()
			break
		}

		ts.point()
		reqList := p.pluckProposal()

		p.slock.unlock()

		p.doProposal(reqList)

		p.slock.lock()

		passTime := ts.point()
		needSleepTime := 0
		if passTime < p.batchDelayTimeMs {
			needSleepTime = p.batchDelayTimeMs - passTime
		}

		if p.needBatch() {
			needSleepTime = 0
		}

		if needSleepTime > 0 {
			p.slock.waitTime(time.Millisecond * time.Duration(needSleepTime))
		}

		p.slock.unlock()
	}

	//notify all waiting thread.
	p.slock.lock()
	defer p.slock.unlock()

	for p.queue.Len() > 0 {
		pp := p.queue.Remove(p.queue.Front()).(*pendingProposal)

		pp.notifier.SendNotify(errPaxosSystemError)
	}

	lPLGHead(p.groupIdx, "Ended.")
}

func (p *proposeBatch) stop() {
	if p.isStarted {
		p.slock.lock()
		p.isEnd = true
		p.slock.broadcast()
		p.slock.unlock()

		if p.quit != nil {
			<-p.quit
			p.quit = nil
		}
	}
}

func (p *proposeBatch) setBatchCount(batchCount int) {
	p.batchCount = batchCount
}

func (p *proposeBatch) setBatchDelayTimeMs(batchDelayTimeMs int) {
	p.batchDelayTimeMs = batchDelayTimeMs
}

func (p *proposeBatch) propose(value []byte, instanceID *uint64, batchIdx *int32, ctx *SMCtx) error {
	if p.isEnd {
		return errPaxosSystemError
	}

	getBPInstance().BatchPropose()

	notifier := p.pool.getNotifier(p.ID)

	p.addProposal(value, instanceID, batchIdx, ctx, notifier)

	err := notifier.WaitNotify()
	if err == nil {
		getBPInstance().BatchProposeOK()
	} else {
		getBPInstance().BatchProposeFail()
	}

	return err
}

func (p *proposeBatch) needBatch() bool {
	if p.queue.Len() >= p.batchCount || p.nowQueueValueSize >= p.batchMaxSize {
		return true
	}

	if p.queue.Len() > 0 {
		pp := p.queue.Front().Value.(*pendingProposal)
		now := getSteadyClockMS()
		var passTime uint64
		if now > pp.absEnqueueTime {
			passTime = now - pp.absEnqueueTime
		}

		if passTime > uint64(p.batchDelayTimeMs) {
			return true
		}
	}

	return false
}

func (p *proposeBatch) addProposal(value []byte, instanceID *uint64, batchIdx *int32, ctx *SMCtx, noti *notifier) {
	p.slock.lock()
	defer p.slock.unlock()

	pp := newPendingProposal()
	pp.notifier = noti
	pp.value = value
	pp.ctx = ctx
	pp.instanceID = instanceID
	pp.batchIndex = batchIdx
	pp.absEnqueueTime = getSteadyClockMS()

	p.queue.PushBack(pp)
	p.nowQueueValueSize += len(pp.value)

	if p.needBatch() {
		lPLGDebug(p.groupIdx, "direct batch, queue size %d value size %d", p.queue.Len(), p.nowQueueValueSize)

		p.doProposal(p.pluckProposal())
	}
}

func (p *proposeBatch) pluckProposal() []*pendingProposal {
	var pluckCount, pluckSize int
	ret := make([]*pendingProposal, 0, 8)
	now := getSteadyClockMS()

	for p.queue.Len() > 0 {
		e := p.queue.Front()
		pp := e.Value.(*pendingProposal)

		ret = append(ret, pp)

		pluckCount++
		pluckSize += len(pp.value)
		p.nowQueueValueSize -= len(pp.value)

		var proposalWaitTime uint64
		if now > pp.absEnqueueTime {
			proposalWaitTime = now - pp.absEnqueueTime
		}
		getBPInstance().BatchProposeWaitTimeMs(int(proposalWaitTime))

		p.queue.Remove(e)

		if pluckCount >= p.batchCount || pluckSize >= p.batchMaxSize {
			break
		}
	}

	if len(ret) > 0 {
		lPLGDebug(p.groupIdx, "pluck %d request", len(ret))
	}

	return ret
}

func (p *proposeBatch) onlyOnePropose(pp *pendingProposal) {
	var err error
	*pp.instanceID, err = p.paxosNode.Propose(p.groupIdx, pp.value, pp.ctx)
	pp.notifier.SendNotify(err)
}

func (p *proposeBatch) doProposal(ps []*pendingProposal) {
	if len(ps) == 0 {
		return
	}

	getBPInstance().BatchProposeDoPropose(len(ps))

	if len(ps) == 1 {
		p.onlyOnePropose(ps[0])
		return
	}

	batchValue := &paxospb.BatchPaxosValues{}
	batchSMCtx := &batchSMCtx{}

	for _, pp := range ps {
		value := &paxospb.PaxosValue{}
		if pp.ctx != nil {
			value.SMID = pp.ctx.SMID
		}
		value.Value = pp.value
		batchValue.Values = append(batchValue.Values, value)

		batchSMCtx.smCtxList = append(batchSMCtx.smCtxList, pp.ctx)
	}

	ctx := &SMCtx{}
	ctx.SMID = batch_Propose_SMID
	ctx.Ctx = batchSMCtx

	var instanceID uint64

	bytes, err := batchValue.Marshal()
	if err == nil {
		//FIXME return err not ret
		instanceID, err = p.paxosNode.Propose(p.groupIdx, bytes, ctx)
		if err != nil {
			lPLGErr(p.groupIdx, "real propose fail, error: %v", err)
		}
	} else {
		lPLGErr(p.groupIdx, "BatchValues Marshal fail")
		err = errPaxosSystemError
	}

	for i, pp := range ps {
		*pp.batchIndex = int32(i)
		*pp.instanceID = instanceID
		pp.notifier.SendNotify(err)
	}
}
