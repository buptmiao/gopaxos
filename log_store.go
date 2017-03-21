package gopaxos

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/buptmiao/gopaxos/paxospb"
)

type logStoreLogger struct {
	f *os.File
}

func newLogStoreLogger() *logStoreLogger {
	return &logStoreLogger{}
}

func (l *logStoreLogger) init(path string) {
	fpath := path + "/LOG"
	l.f, _ = os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

func (l *logStoreLogger) log(format string, args ...interface{}) {
	if l.f == nil {
		return
	}
	now := time.Now()
	timePrefix := now.Format("2006-Jan-2 15:04:05")
	newFormat := fmt.Sprintf("%s:%d %s\n", timePrefix, now.Unix(), format)
	str := fmt.Sprintf(newFormat, args...)
	n, _ := l.f.WriteString(str)
	if n != len(str) {
		lPLErr("fail, len %d writelen %d", len(str), n)
	}
}

func (l *logStoreLogger) close() {
	if l.f != nil {
		l.f.Close()
		l.f = nil
	}
}

type logStore struct {
	fd           *os.File
	metaFd       *os.File
	fileID       int64
	path         string
	tmpBuf       []byte
	tmpAppendBuf []byte
	mu           sync.Mutex
	rdMu         sync.Mutex

	maxDeletedFileID int64
	groupIdx         int
	nowFileSize      int64
	nowFileOffset    int64

	timeStat   timeStat
	fileLogger *logStoreLogger
}

func newLogStore() *logStore {
	return &logStore{
		fileID:           -1,
		maxDeletedFileID: -1,
		groupIdx:         -1,
		nowFileSize:      -1,
		nowFileOffset:    0,
		fileLogger:       newLogStoreLogger(),
	}
}

func (l *logStore) init(dbPath string, groupIdx int, d *db) error {
	l.groupIdx = groupIdx
	l.path = dbPath + "/" + "vfile"
	if _, err := os.Stat(l.path); os.IsNotExist(err) {
		if err = os.Mkdir(l.path, 0775); err != nil {
			lPLGErr(l.groupIdx, "Create dir fail, path %s", l.path)
			return err
		}
	}

	l.fileLogger.init(l.path)

	metaPath := l.path + "/meta"

	var err error
	l.metaFd, err = os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		lPLGErr(l.groupIdx, "open meta file fail, filepath %s", metaPath)
		return err
	}

	_, err = l.metaFd.Seek(0, os.SEEK_SET)
	if err != nil {
		lPLGErr(l.groupIdx, "seek meta file fail, filepath %s", metaPath)
		return err
	}

	fileIDBuf := make([]byte, 8)
	readLen, _ := l.metaFd.Read(fileIDBuf)
	l.fileID = int64(binary.LittleEndian.Uint64(fileIDBuf))
	if readLen != intlen {
		if readLen == 0 {
			l.fileID = 0
		} else {
			lPLGErr(l.groupIdx, "read meta info fail, readlen %d", readLen)
			return io.EOF
		}
	}

	checksumBuf := make([]byte, 4)
	readLen, _ = l.metaFd.Read(checksumBuf)
	metaChecksum := binary.LittleEndian.Uint32(checksumBuf)
	if readLen == 4 {
		checksum := crc(0, fileIDBuf)
		if checksum != metaChecksum {
			lPLGErr(l.groupIdx, "meta file checksum %d not same to cal checksum %d, fileid %d",
				metaChecksum, checksum, l.fileID)
			return errChecksumNotMatch
		}
	}

	l.nowFileOffset, err = l.rebuildIndex(d)
	if err != nil {
		lPLGErr(l.groupIdx, "rebuild index fail, ret %v", err)
		return err
	}

	l.fd, err = l.openFile(l.fileID)
	if err != nil {
		lPLGErr(l.groupIdx, "open file fail, ret %v", err)
		return err
	}

	l.nowFileSize, err = l.expandFile(l.fd)
	if err != nil {
		lPLGErr(l.groupIdx, "expand file fail, ret %v", err)
		return err
	}

	l.nowFileOffset, err = l.fd.Seek(l.nowFileOffset, os.SEEK_SET)
	if err != nil {
		lPLGErr(l.groupIdx, "seek to now file offset %d fail", l.nowFileOffset)
		return err
	}

	l.fileLogger.log("init write fileid %d now_w_offset %d filesize %d",
		l.fileID, l.nowFileOffset, l.nowFileSize)

	lPLGHead(l.groupIdx, "ok, path %s fileid %d meta checksum %d nowfilesize %d nowfilewriteoffset %d",
		l.path, l.fileID, metaChecksum, l.nowFileSize, l.nowFileOffset)

	return nil
}

func (l *logStore) expandFile(fd *os.File) (int64, error) {
	fileSize, err := fd.Seek(0, os.SEEK_END)
	if err != nil {
		lPLGErr(l.groupIdx, "lseek fail, ret %d", fileSize)
		return 0, err
	}

	if fileSize == 0 {
		// new file
		fileSize, err = fd.Seek(int64(getInsideOptionsInstance().getLogFileMaxSize()-1), os.SEEK_SET)
		if err != nil {
			return 0, err
		}

		_, err = fd.Write(make([]byte, 1))
		if err != nil {
			lPLGErr(l.groupIdx, "write 1 bytes fail")
			return 0, err
		}

		fileSize = int64(getInsideOptionsInstance().getLogFileMaxSize())
		_, err = fd.Seek(0, os.SEEK_SET)
		l.nowFileOffset = 0
		if err != nil {
			return 0, err
		}
	}

	return fileSize, nil
}

func (l *logStore) increaseFileID() error {
	fileID := l.fileID + 1
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(fileID))
	checkSum := crc(0, buf)

	_, err := l.metaFd.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	_, err = l.metaFd.Write(buf)
	if err != nil {
		lPLGErr(l.groupIdx, "write meta fileid fail, %v", err)
		return err
	}

	checksumBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBuf, checkSum)
	_, err = l.metaFd.Write(checksumBuf)
	if err != nil {
		lPLGErr(l.groupIdx, "write meta checksum fail, %v", err)
		return err
	}

	err = l.metaFd.Sync()
	if err != nil {
		return err
	}

	l.fileID++

	return nil
}

func (l *logStore) openFile(fileID int64) (*os.File, error) {
	filepath := fmt.Sprintf("%s/%d.f", l.path, fileID)
	fd, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lPLGErr(l.groupIdx, "open fail fail, filepath %s", filepath)
		return nil, err
	}

	lPLGImp(l.groupIdx, "ok, path %s", filepath)
	return fd, nil
}

func (l *logStore) deleteFile(fileID int64) error {
	if l.maxDeletedFileID == -1 {
		if fileID-2000 > 0 {
			l.maxDeletedFileID = fileID - 2000
		}
	}

	if fileID <= l.maxDeletedFileID {
		lPLGDebug(l.groupIdx, "file already deleted, fileid %d deletedmaxfileid %d",
			fileID, l.maxDeletedFileID)
		return nil
	}

	for deleteFileID := l.maxDeletedFileID; deleteFileID <= fileID; deleteFileID++ {
		filepath := fmt.Sprintf("%s/%d.f", l.path, fileID)
		if _, err := os.Stat(filepath); os.IsNotExist(err) {
			lPLGDebug(l.groupIdx, "file already deleted, filepath %s", filepath)
			l.maxDeletedFileID = deleteFileID
			continue
		}

		if err := os.Remove(filepath); err != nil {
			lPLGErr(l.groupIdx, "remove fail, filepath %s ret %v", filepath, err)
			return err
		}

		l.maxDeletedFileID = deleteFileID
		l.fileLogger.log("delete fileid %d", deleteFileID)
	}
	return nil
}

func (l *logStore) getFileID(size int) (*os.File, int64, int64, error) {
	if l.fd == nil {
		lPLGErr(l.groupIdx, "File aready broken, fileid %d", l.fileID)
		return nil, 0, 0, errFileBroken
	}

	offset, err := l.fd.Seek(l.nowFileOffset, os.SEEK_SET)
	if err != nil {
		lPLGErr(l.groupIdx, "seek file failed: %v ", err)
		return nil, 0, 0, err
	}

	if offset+int64(size) > l.nowFileSize {
		l.fd.Close()
		l.fd = nil

		if err := l.increaseFileID(); err != nil {
			l.fileLogger.log("new file increase fileid fail, now fileid %d", l.fileID)
			return nil, 0, 0, err
		}

		l.fd, err = l.openFile(l.fileID)
		if err != nil {
			l.fileLogger.log("new file open file fail, now fileid %d", l.fileID)
			return nil, 0, 0, err
		}

		offset, err = l.fd.Seek(0, os.SEEK_END)
		if offset != 0 {
			if err != nil {
				lPLGErr(l.groupIdx, "seek file failed: %v ", offset)
				return nil, 0, 0, err
			}

			l.fileLogger.log("new file but file aready exist, now fileid %d exist filesize %d", l.fileID, offset)

			lPLGErr(l.groupIdx, "IncreaseFileID success, but file exist, data wrong, file size %d", offset)
			return nil, 0, 0, errGetFileID
		}

		l.nowFileSize, err = l.expandFile(l.fd)
		if err != nil {
			lPLGErr(l.groupIdx, "new file expand fail, fileid %d filename %s", l.fileID, l.fd.Name())

			l.fileLogger.log("new file expand file fail, now fileid %d", l.fileID)

			l.fd.Close()
			l.fd = nil
			return nil, 0, 0, err
		}

		l.fileLogger.log("new file expand ok, fileid %d filesize %d", l.fileID, l.nowFileSize)
	}

	return l.fd, l.fileID, offset, nil
}

func (l *logStore) append(wo writeOptions, instanceID uint64, buf []byte) (string, error) {
	l.timeStat.point()
	l.mu.Lock()
	defer l.mu.Unlock()

	bufLen := uint64(len(buf) + 8)
	tmpBufLen := bufLen + 8
	fd, fileID, offset, err := l.getFileID(int(tmpBufLen))
	if err != nil {
		return "", err
	}

	l.tmpAppendBuf = make([]byte, 0, tmpBufLen)
	byteBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteBuf, bufLen)
	l.tmpAppendBuf = append(l.tmpAppendBuf, byteBuf...)
	binary.LittleEndian.PutUint64(byteBuf, instanceID)
	l.tmpAppendBuf = append(l.tmpAppendBuf, byteBuf...)
	l.tmpAppendBuf = append(l.tmpAppendBuf, buf...)

	writeLen, err := fd.Write(l.tmpAppendBuf)
	if err != nil {
		getBPInstance().AppendDataFail()
		lPLGErr(l.groupIdx, "writelen %d not equal to %d, buffersize %d, err: %v",
			writeLen, tmpBufLen, len(buf), err)
		return "", err
	}

	if wo {
		err := fd.Sync()
		if err != nil {
			lPLGErr(l.groupIdx, "fdatasync fail, writelen %d errno %v", writeLen, err)
			return "", err
		}
	}

	l.nowFileOffset += int64(writeLen)

	useTimeMs := l.timeStat.point()

	getBPInstance().AppendDataOK(writeLen, useTimeMs)

	checksum := crc(0, l.tmpAppendBuf[8:])

	sFileID := l.genFileID(fileID, offset, checksum)

	lPLGImp(l.groupIdx, "ok, offset %d fileid %d checksum %d instanceid %d buffer size %d usetime %dms sync %v",
		offset, fileID, checksum, instanceID, len(buf), useTimeMs, wo)

	return sFileID, nil
}

func (l *logStore) genFileID(fileID, offset int64, checksum uint32) string {
	ret := make([]byte, 8+8+4)
	binary.LittleEndian.PutUint64(ret, uint64(fileID))
	binary.LittleEndian.PutUint64(ret[8:], uint64(offset))
	binary.LittleEndian.PutUint32(ret[16:], checksum)
	return string(ret)
}

func (l *logStore) parseFileID(sFileID string) (int64, int64, uint32) {
	buf := []byte(sFileID)
	fileID := int64(binary.LittleEndian.Uint64(buf))
	offset := int64(binary.LittleEndian.Uint64(buf[8:]))
	checksum := binary.LittleEndian.Uint32(buf[16:])
	return fileID, offset, checksum
}

func (l *logStore) isValidFileID(sFileID string) bool {
	if len(sFileID) != 8+8+4 {
		return false
	}
	return true
}

func (l *logStore) read(sFileID string) ([]byte, uint64, error) {
	fileID, offset, checksum := l.parseFileID(sFileID)
	fd, err := l.openFile(fileID)
	if err != nil {
		lPLGErr(l.groupIdx, "open file failed: %v ", err)
		return nil, 0, err
	}

	_, err = fd.Seek(offset, os.SEEK_SET)
	if err != nil {
		lPLGErr(l.groupIdx, "seek file failed: %v ", err)
		return nil, 0, err
	}

	tmpBuf := make([]byte, 8)
	n, err := fd.Read(tmpBuf)
	if err != nil {
		fd.Close()
		lPLGErr(l.groupIdx, "readlen %d not qual to 8, err: %v", n, err)
		return nil, 0, err
	}
	bufLen := binary.LittleEndian.Uint64(tmpBuf)

	l.rdMu.Lock()
	defer l.rdMu.Unlock()

	l.tmpBuf = make([]byte, bufLen)
	readLen, err := fd.Read(tmpBuf)
	if err != nil {
		fd.Close()
		lPLGErr(l.groupIdx, "readlen %d not qual to %d", readLen, bufLen)
		return nil, 0, err
	}

	fd.Close()

	if fileChecksum := crc(0, tmpBuf); fileChecksum != checksum {
		getBPInstance().GetFileChecksumNotEqual()
		lPLGErr(l.groupIdx, "checksum not equal, filechecksum %d checksum %d", fileChecksum, checksum)
		return nil, 0, errChecksumNotMatch
	}

	instanceID := binary.LittleEndian.Uint64(tmpBuf)
	buf := tmpBuf[8:]
	lPLGImp(l.groupIdx, "ok, fileid %d offset %d instanceid %d buffer size %d",
		fileID, offset, instanceID, bufLen-8)

	return buf, instanceID, nil
}

func (l *logStore) del(sFileID string, instanceID uint64) error {
	fileID, _, _ := l.parseFileID(sFileID)

	if fileID > l.fileID {
		lPLGErr(l.groupIdx, "del fileid %d large than useing fileid %d", fileID, l.fileID)
		return errFileIDTooLarge
	}

	if fileID > 0 {
		return l.deleteFile(fileID)
	}

	return nil
}

func (l *logStore) forceDel(sFileID string, instanceID uint64) error {
	fileID, offset, _ := l.parseFileID(sFileID)

	if fileID != l.fileID {
		lPLGErr(l.groupIdx, "del fileid %d not equal to fileid %d", fileID, l.fileID)
		return errFileIDMismatch
	}

	filepath := fmt.Sprintf("%s/%d.f", l.path, fileID)

	fmt.Println("fileid %d offset %d", fileID, offset)

	return os.Truncate(filepath, offset)
}

func (l *logStore) rebuildIndex(d *db) (int64, error) {
	var nowFileWriteOffset int64
	lastFileID, nowInstanceID, err := d.getMaxInstanceIDFileID()
	if err != nil {
		return nowFileWriteOffset, err
	}
	var fileID, offset int64
	var checksum uint32
	if l.isValidFileID(lastFileID) {
		fileID, offset, checksum = l.parseFileID(lastFileID)
	}

	if fileID > l.fileID {
		lPLGErr(l.groupIdx, "LevelDB last fileid %d larger than meta now fileid %d, file error",
			fileID, l.fileID)
		return nowFileWriteOffset, errFileIDTooLarge
	}

	lPLGHead(l.groupIdx, "START fileid %d offset %d checksum %d", fileID, offset, checksum)

	for nowFileID := fileID; ; nowFileID++ {
		nowFileWriteOffset, err = l.rebuildIndexForOneFile(nowFileID, offset, d, &nowInstanceID)
		if err != nil && err != errFileNotExist {
			return nowFileWriteOffset, err
		}
		if err == errFileNotExist {
			if nowFileID != 0 && nowFileID != l.fileID+1 {
				lPLGErr(l.groupIdx, "meta file wrong, nowfileid %d meta.nowfileid %d", nowFileID, l.fileID)
				return nowFileWriteOffset, errMetaFileBroken
			}
			lPLGImp(l.groupIdx, "END rebuild ok, nowfileid %d", nowFileID)
			return nowFileWriteOffset, nil
		}
		offset = 0
	}
	return nowFileWriteOffset, nil
}

func (l *logStore) rebuildIndexForOneFile(fileID int64, offset int64, d *db, nowInstanceID *uint64) (int64, error) {
	var nowFileWriteOffset int64
	var err error
	filepath := fmt.Sprintf("%s/%d.f", l.path, fileID)

	if _, e := os.Stat(filepath); os.IsNotExist(e) {
		lPLGDebug(l.groupIdx, "file not exist, filepath %s", filepath)
		return 0, errFileNotExist
	}

	fd, err := l.openFile(fileID)
	if err != nil {
		return 0, err
	}

	fileInfo, err := fd.Stat()
	if err != nil {
		fd.Close()
		return 0, err
	}
	fileLen := fileInfo.Size()

	_, err = fd.Seek(offset, os.SEEK_SET)
	if err != nil {
		fd.Close()
		return 0, err
	}
	nowOffset := offset
	var needTruncate bool
	tmpBuf := make([]byte, 8)
	for {
		readLen, _ := fd.Read(tmpBuf)
		if readLen == 0 {
			lPLGHead(l.groupIdx, "File End, fileid %d offset %d", fileID, nowOffset)
			nowFileWriteOffset = nowOffset
			break
		}
		if readLen != 8 {
			needTruncate = true
			lPLGErr(l.groupIdx, "readlen %d not qual to 8, need truncate", readLen)
			break
		}

		bufLen := binary.LittleEndian.Uint64(tmpBuf)
		if bufLen == 0 {
			lPLGHead(l.groupIdx, "File Data End, fileid %d offset %d", fileID, nowOffset)
			nowFileWriteOffset = nowOffset
			break
		}

		if bufLen > uint64(fileLen) || bufLen < 8 {
			lPLGErr(l.groupIdx, "File data len wrong, data len %d filelen %d", bufLen, fileLen)
			err = errFileSizeWrong
			break
		}

		l.tmpBuf = make([]byte, bufLen)
		readLen, _ = fd.Read(l.tmpBuf)
		if uint64(readLen) != bufLen {
			needTruncate = true
			lPLGErr(l.groupIdx, "readlen %d not qual to %d, need truncate", readLen, bufLen)
			break
		}

		instanceID := binary.LittleEndian.Uint64(l.tmpBuf[:8])

		if instanceID < *nowInstanceID {
			lPLGErr(l.groupIdx, "File data wrong, read instanceid %d smaller than now instanceid %d",
				instanceID, nowInstanceID)
			err = errFileBroken
			break
		}

		*nowInstanceID = instanceID

		state := &paxospb.AcceptorStateData{}
		e := state.Unmarshal(l.tmpBuf[8:])
		if e != nil {
			l.nowFileOffset = nowOffset
			lPLGErr(l.groupIdx, "This instance's buffer wrong, can't parse to acceptState, instanceid %d bufferlen %d nowoffset %d",
				instanceID, bufLen-8, nowOffset)
			needTruncate = true
			break
		}

		fileChecksum := crc(0, l.tmpBuf)

		sFileID := l.genFileID(fileID, nowOffset, fileChecksum)

		err = d.rebuildOneIndex(instanceID, sFileID)
		if err != nil {
			break
		}
		lPLGImp(l.groupIdx, "rebuild one index ok, fileid %d offset %d instanceid %d checksum %d buffer size %d",
			fileID, nowOffset, instanceID, fileChecksum, bufLen-8)

		nowOffset += int64(bufLen) + 8
	}

	fd.Close()

	if needTruncate {
		l.fileLogger.log("truncate fileid %d offset %d filesize %d",
			fileID, nowOffset, fileLen)
		if e := os.Truncate(filepath, nowOffset); e != nil {
			lPLGErr(l.groupIdx, "truncate fail, file path %s truncate to length %d err: %v",
				filepath, nowOffset, e)
			return nowFileWriteOffset, e
		}
	}

	return nowFileWriteOffset, err
}

func (l *logStore) close() {
	if l.fd != nil {
		l.fd.Close()
		l.fd = nil
	}

	if l.metaFd != nil {
		l.metaFd.Close()
		l.metaFd = nil
	}
}
