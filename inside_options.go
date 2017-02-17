package gopaxos

import (
	"github.com/docker/docker/pkg/random"
	"sync"
)

var oneInsideOptions sync.Once
var dfInsideOptions *insideOptions

func getInsideOptionsInstance() *insideOptions {
	oneInsideOptions.Do(initInsideOptions)
	return dfInsideOptions
}

func initInsideOptions() {
	dfInsideOptions = &insideOptions{
		isLargeBufferMode: false,
		isFollower:        false,
		groupCount:        1,
	}
}

type insideOptions struct {
	isLargeBufferMode bool
	isFollower        bool
	groupCount        int
}

func (i *insideOptions) setAsLargeBufferMode() {
	i.isLargeBufferMode = true
}

func (i *insideOptions) setAsFollower() {
	i.isFollower = true
}

func (i *insideOptions) setGroupCount(groupCount int) {
	i.groupCount = groupCount
}

func (i *insideOptions) getMaxBufferSize() int {
	if i.isLargeBufferMode {
		return 52428800
	}
	return 10485760
}

func (i *insideOptions) getStartPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 15000
	}
	return 2000
}

func (i *insideOptions) getStartAcceptTimeoutMs() int {
	if i.isLargeBufferMode {
		return 15000
	}
	return 1000
}

func (i *insideOptions) getMaxPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 90000
	}
	return 8000
}

func (i *insideOptions) getMaxAcceptTimeoutMs() int {
	if i.isLargeBufferMode {
		return 90000
	}
	return 8000
}

func (i *insideOptions) getMaxIOLoopQueueLen() int {
	if i.isLargeBufferMode {
		return 1024/i.groupCount + 100
	}
	return 10240/i.groupCount + 1000
}

func (i *insideOptions) getMaxQueueLen() int {
	if i.isLargeBufferMode {
		return 1024
	}
	return 10240
}

func (i *insideOptions) getAskforLearnInterval() int {
	if !i.isFollower {
		if i.isLargeBufferMode {
			return 50000 + random.Rand.Int()%10000
		}
		return 2500 + random.Rand.Int()%500
	}
	if i.isLargeBufferMode {
		return 30000 + random.Rand.Int()%15000
	}
	return 2000 + random.Rand.Int()%1000
}

func (i *insideOptions) getLearnerReceiverAckLead() int {
	if i.isLargeBufferMode {
		return 2
	}
	return 4
}

func (i *insideOptions) getLearnerSenderPrepareTimeoutMs() int {
	if i.isLargeBufferMode {
		return 6000
	}
	return 5000
}

func (i *insideOptions) getLearnerSenderAckTimeoutMs() int {
	if i.isLargeBufferMode {
		return 60000
	}
	return 5000
}

func (i *insideOptions) getLearnerSenderAckLead() int {
	if i.isLargeBufferMode {
		return 5
	}
	return 21
}

func (i *insideOptions) getTcpOutQueueDropTimeMs() int {
	if i.isLargeBufferMode {
		return 20000
	}
	return 5000
}

func (i *insideOptions) getLogFileMaxSize() int {
	if i.isLargeBufferMode {
		return 524288000
	}
	return 104857600
}

func (i *insideOptions) getTcpConnectionNonActiveTimeout() int {
	if i.isLargeBufferMode {
		return 600000
	}
	return 60000
}

func (i *insideOptions) getLearnerSenderSendQps() int {
	if i.isLargeBufferMode {
		return 10000 / i.groupCount
	}
	return 100000 / i.groupCount
}

func (i *insideOptions) getCleanerDeleteQps() int {
	if i.isLargeBufferMode {
		return 30000 / i.groupCount
	}
	return 300000 / i.groupCount
}
