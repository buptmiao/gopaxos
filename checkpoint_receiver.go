package gopaxos

import (
	"fmt"
	"github.com/buptmiao/gopaxos/paxospb"
	"io/ioutil"
	"os"
	"strings"
)

type checkpointReceiver struct {
	conf          *config
	logStorage    LogStorage
	senderNodeID  uint64
	uuid          uint64
	sequence      uint64
	hasInitDirMap map[string]bool
}

func newCheckpointReceiver(conf *config, ls LogStorage) *checkpointReceiver {
	return &checkpointReceiver{
		conf:          conf,
		logStorage:    ls,
		hasInitDirMap: make(map[string]bool),
	}
}

func (c *checkpointReceiver) reset() {
	c.hasInitDirMap = make(map[string]bool)
	c.senderNodeID = nullNode
	c.uuid = 0
	c.sequence = 0
}

func (c *checkpointReceiver) newReceiver(senderNodeID uint64, uuid uint64) error {
	if err := c.clearCheckpointTmp(); err != nil {
		return err
	}

	if err := c.logStorage.ClearAllLog(c.conf.groupIdx); err != nil {
		lPLGErr(c.conf.groupIdx, "ClearAllLog fail, groupidx %d, error: %v",
			c.conf.getMyGroupIdx(), err)
		return err
	}

	c.hasInitDirMap = make(map[string]bool)
	c.senderNodeID = senderNodeID
	c.uuid = uuid
	c.sequence = 0

	return nil
}

func (c *checkpointReceiver) clearCheckpointTmp() error {
	logPath := c.logStorage.GetLogStorageDirPath(c.conf.groupIdx)

	files, err := ioutil.ReadDir(logPath)
	if err != nil {
		lPLGErr(c.conf.groupIdx, "read dir failed, error: %v", err)
		return err
	}

	for _, f := range files {
		if strings.Contains(f.Name(), "cp_tmp_") {
			childpath := fmt.Sprintf("%s/%s", logPath, f.Name())
			err = deleteDir(childpath)
			if err != nil {
				break
			}

			lPLGHead(c.conf.groupIdx, "rm dir %s done!", childpath)
		}
	}

	return err
}

func (c *checkpointReceiver) isReceiverFinish(senderNodeID uint64, uuid uint64, endSequence uint64) bool {
	if senderNodeID == c.senderNodeID && uuid == c.uuid && endSequence == c.sequence+1 {
		return true
	}

	return false
}

func (c *checkpointReceiver) getTmpDirPath(smID int64) string {
	logPath := c.logStorage.GetLogStorageDirPath(c.conf.groupIdx)

	return fmt.Sprintf("%s/cp_tmp_%d", logPath, smID)
}

func (c *checkpointReceiver) initFilePath(filePath string) (string, error) {
	lPLGHead(c.conf.groupIdx, "START filepath %s", filePath)
	newPath := "/" + filePath + "/"
	tmpList := strings.Split(newPath, "/")
	dirList := make([]string, 0, len(tmpList))
	for _, v := range tmpList {
		if v != "" {
			dirList = append(dirList, v)
		}
	}

	formatFilePath := "/"
	for i, v := range dirList {
		if i+1 == len(dirList) {
			formatFilePath += v
		} else {
			formatFilePath += v + "/"
			if _, ok := c.hasInitDirMap[formatFilePath]; !ok {
				err := c.createDir(formatFilePath)
				if err != nil {
					return "", err
				}

				c.hasInitDirMap[formatFilePath] = true
			}
		}
	}

	lPLGImp(c.conf.groupIdx, "ok, format filepath %s", formatFilePath)

	return formatFilePath, nil
}

func (c *checkpointReceiver) createDir(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err = os.Mkdir(dirPath, 0775); err != nil {
			lPLGErr(c.conf.groupIdx, "Create dir fail, path %s", dirPath)
			return err
		}
	}

	return nil
}

func (c *checkpointReceiver) receiveCheckpoint(checkpointMsg *paxospb.CheckpointMsg) error {
	if checkpointMsg.GetNodeID() != c.senderNodeID || checkpointMsg.GetUUID() != c.uuid {
		lPLGErr(c.conf.groupIdx, "msg not valid, Msg.SenderNodeID %d Receiver.SenderNodeID %d Msg.UUID %lu Receiver.UUID %d",
			checkpointMsg.GetNodeID(), c.senderNodeID, checkpointMsg.GetUUID(), c.uuid)
		return errMsgNotValid
	}

	if checkpointMsg.GetSequence() == c.sequence {
		lPLGErr(c.conf.groupIdx, "msg already receive, skip, Msg.Sequence %d Receiver.Sequence %d",
			checkpointMsg.GetSequence(), c.sequence)
		return nil
	}

	if checkpointMsg.GetSequence() != c.sequence+1 {
		lPLGErr(c.conf.groupIdx, "msg sequence wrong, Msg.Sequence %d Receiver.Sequence %d",
			checkpointMsg.GetSequence(), c.sequence)
		return errMsgSequenceWrong
	}

	filePath := c.getTmpDirPath(checkpointMsg.GetSMID()) + "/" + checkpointMsg.GetFilePath()
	formatFilePath, err := c.initFilePath(filePath)
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(formatFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		lPLGErr(c.conf.groupIdx, "open file fail, filepath %s", formatFilePath)
		return err
	}

	defer fd.Close()
	fileOffset, err := fd.Seek(0, os.SEEK_END)
	if fileOffset != int64(checkpointMsg.GetOffset()) {
		lPLGErr(c.conf.groupIdx, "file.offset %d not equal to msg.offset %d, error: %v", fileOffset, checkpointMsg.GetOffset(), err)
		return errFileOffsetMismatch
	}

	writeLen, err := fd.Write(checkpointMsg.GetBuffer())
	if err != nil {
		lPLGImp(c.conf.groupIdx, "write fail, writelen %d buffer size %d", writeLen, len(checkpointMsg.GetBuffer()))
		return err
	}

	c.sequence++
	lPLGImp(c.conf.groupIdx, "END ok, writelen %d", writeLen)

	return nil
}
