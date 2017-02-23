package gopaxos

import (
	"math/rand"
	"time"
)

const (
	canDeleteDelta     = 1000000
	deleteSaveInterval = 100
)

type cleaner struct {
	conf       *config
	smFac      *smFac
	logStorage LogStorage
	cpMgr      *checkpointMgr
	lastSave   uint64
	canRun     bool
	isPause    bool
	isEnd      bool
	isStart    bool
	holdCount  uint64
	quit       chan struct{}
}

func newCleaner(conf *config, smFac *smFac, ls LogStorage, cpMgr *checkpointMgr) *cleaner {
	return &cleaner{
		conf:       conf,
		smFac:      smFac,
		logStorage: ls,
		cpMgr:      cpMgr,
		isPause:    true,
		holdCount:  canDeleteDelta,
	}
}

func (c *cleaner) start() {
	c.quit = make(chan struct{}, 1)
	go c.run()
}

func (c *cleaner) stop() {
	c.isEnd = true
	if c.isStart && c.quit != nil {
		<-c.quit
	}
}

func (c *cleaner) pause() {
	c.canRun = false
}

func (c *cleaner) resume() {
	c.isPause = false
	c.canRun = true
}

func (c *cleaner) isPaused() bool {
	return c.isPause
}

func (c *cleaner) run() {
	c.isStart = true
	c.resume()

	//control delete speed to avoid affecting the io too much.
	deleteOps := getInsideOptionsInstance().getCleanerDeleteQps()
	sleepMs, deleteInterval := 0, 0
	if deleteOps > 1000 {
		sleepMs = 1
		deleteInterval = deleteOps/1000 + 1
	} else {
		sleepMs = 1000 / deleteOps
		deleteInterval = 1
	}

	lPLGDebug(c.conf.groupIdx, "DeleteQps %d SleepMs %d DeleteInterval %d", deleteOps, sleepMs, deleteInterval)

	for {
		if c.isEnd {
			lPLGHead(c.conf.groupIdx, "Checkpoint.Cleaner [END]")
			close(c.quit)
			c.quit = nil
			return
		}

		if !c.canRun {
			lPLGImp(c.conf.groupIdx, "Pausing, sleep")
			c.isPause = true
			time.Sleep(time.Second)
			continue
		}

		instanceID := c.cpMgr.getMinChosenInstanceID()
		cpInstanceID := c.smFac.getCheckpointInstanceID(c.conf.groupIdx) + 1
		maxChosenInstanceID := c.cpMgr.getMaxChosenInstanceID()

		deleteCount := 0
		for instanceID+c.holdCount < cpInstanceID && instanceID+c.holdCount < maxChosenInstanceID {
			if c.deleteOne(instanceID) {
				instanceID++
				deleteCount++
				if deleteCount >= deleteInterval {
					deleteCount = 0
					time.Sleep(time.Millisecond * sleepMs)
				}
			} else {
				lPLGDebug(c.conf.groupIdx, "delete system fail, instanceid %d", instanceID)
				break
			}
		}

		if cpInstanceID == 0 {
			lPLGStatus(c.conf.groupIdx, "sleep a while, max deleted instanceid %d checkpoint instanceid (no checkpoint) now instanceid %d",
				instanceID, c.cpMgr.getMaxChosenInstanceID())
		} else {
			lPLGStatus(c.conf.groupIdx, "sleep a while, max deleted instanceid %d checkpoint instanceid %d now instanceid %d",
				instanceID, cpInstanceID, c.cpMgr.getMaxChosenInstanceID())
		}

		time.Sleep(time.Millisecond * (rand.Uint32()%500 + 500))
	}
}

func (c *cleaner) fixMinChosenInstanceID(oldMinChosenInstanceID uint64) error {
	cpInstanceID := c.smFac.getCheckpointInstanceID(c.conf.groupIdx) + 1
	fixMinChosenInstanceID := oldMinChosenInstanceID

	for instanceID := oldMinChosenInstanceID; instanceID < oldMinChosenInstanceID+deleteSaveInterval; instanceID++ {
		if instanceID >= cpInstanceID {
			break
		}

		_, err := c.logStorage.Get(c.conf.groupIdx, instanceID)
		if err != nil && err != ErrNotFoundFromStorage {
			return err
		} else if err == ErrNotFoundFromStorage {
			fixMinChosenInstanceID = instanceID + 1
		} else {
			break
		}
	}

	if fixMinChosenInstanceID > oldMinChosenInstanceID {
		if err := c.cpMgr.setMinChosenInstanceID(fixMinChosenInstanceID); err != nil {
			lPLGErr(c.conf.groupIdx, "set min chosen instance ID failed, error: %v", err)
			return err
		}
	}

	lPLGImp(c.conf.groupIdx, "ok, old minchosen %d fix minchosen %d", oldMinChosenInstanceID, fixMinChosenInstanceID)

	return nil
}

func (c *cleaner) deleteOne(instanceID uint64) bool {
	wo := writeOptions(false)

	if err := c.logStorage.Del(wo, c.conf.groupIdx, instanceID); err != nil {
		return false
	}

	c.cpMgr.setMinChosenInstanceIDCache(instanceID)

	if instanceID >= c.lastSave+deleteSaveInterval {
		if err := c.cpMgr.setMinChosenInstanceID(instanceID + 1); err != nil {
			lPLGErr(c.conf.groupIdx, "SetMinChosenInstanceID fail, now delete instanceid %d", instanceID)
			return false
		}

		c.lastSave = instanceID

		lPLGImp(c.conf.groupIdx, "delete %d instance done, now minchosen instanceid %d", deleteSaveInterval, instanceID+1)
	}

	return true
}

func (c *cleaner) setHoldPaxosLogCount(holdCount uint64) {
	if holdCount < 300 {
		c.holdCount = 300
	} else {
		c.holdCount = holdCount
	}
}
