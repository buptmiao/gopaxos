package gopaxos

import (
	"encoding/binary"
	"github.com/buptmiao/gopaxos/paxospb"
)

type ballotNumber struct {
	proposalID uint64
	nodeID     uint64
}

func newBallotNumber(pid uint64, nid uint64) *ballotNumber {
	return &ballotNumber{
		proposalID: pid,
		nodeID:     nid,
	}
}

func (b *ballotNumber) isnull() bool {
	return b.proposalID == 0
}

func (b *ballotNumber) reset() {
	b.proposalID = 0
	b.nodeID = 0
}

func (b *ballotNumber) gte(other *ballotNumber) bool {
	if b.proposalID == other.proposalID {
		return b.nodeID >= other.nodeID
	}
	return b.proposalID >= other.proposalID
}

func (b *ballotNumber) ne(other *ballotNumber) bool {
	return b.proposalID != other.proposalID || b.nodeID != other.nodeID
}

func (b *ballotNumber) equal(other *ballotNumber) bool {
	return *b == *other
}

func (b *ballotNumber) gt(other *ballotNumber) bool {
	if b.proposalID == other.proposalID {
		return b.nodeID > other.nodeID
	}
	return b.proposalID > other.proposalID
}

///////////////////////////////////////////////////////////////////////////////
type broadcastMessage_Type int32

const (
	broadcastMessage_Type_RunSelf_First broadcastMessage_Type = 1 + iota
	broadcastMessage_Type_RunSelf_Final
	broadcastMessage_Type_RunSelf_None
)

type base struct {
	conf       *config
	comm       MsgTransport
	instance   *instance
	instanceID uint64
	isTestMode bool
}

func newBase(conf *config, tran MsgTransport, i *instance) base {
	ret := base{}
	ret.conf = conf
	ret.comm = tran
	ret.instance = i
	ret.instanceID = 0
	ret.isTestMode = false

	return ret
}

func (b base) getInstanceID() uint64 {
	return b.instanceID
}

func (b base) setInstanceID(instanceID uint64) {
	b.instanceID = instanceID
}

// it must be implemented by derived struct
//func (b base) newInstance() {
//	b.instanceID++
//	b.initForNewPaxosInstance()
//}

func (b base) getLastChecksum() uint32 {
	return b.instance.getLastChecksum()
}

func (b base) packMsg(paxosMsg *paxospb.PaxosMsg) ([]byte, error) {
	body, err := paxosMsg.Marshal()
	if err != nil {
		lPLGErr(b.conf.groupIdx, "PaxosMsg.Marshal fail, skip this msg")
		return nil, err
	}

	cmd := msgCmd_PaxosMsg

	return b.packBaseMsg(body, cmd), nil
}

func (b base) packCheckpointMsg(checkpointMsg *paxospb.CheckpointMsg) ([]byte, error) {
	body, err := checkpointMsg.Marshal()
	if err != nil {
		lPLGErr(b.conf.groupIdx, "CheckpointMsg.Marshal fail, skip this msg")
		return nil, err
	}

	cmd := msgCmd_CheckpointMsg

	return b.packBaseMsg(body, cmd), nil
}

func (b base) packBaseMsg(body []byte, cmd msgCmd) []byte {
	groupIdx := make([]byte, 8)
	binary.LittleEndian.PutUint64(groupIdx, uint64(b.conf.groupIdx))

	header := &paxospb.Header{}
	header.Gid = b.conf.getGid()
	header.Rid = 0
	header.Cmdid = int32(cmd)
	header.Version = 1

	headerBuf, err := header.Marshal()
	if err != nil {
		lPLGErr(b.conf.groupIdx, "Header.Marshal fail, skip this msg")
	}

	headerLen := make([]byte, headlen_len)
	binary.LittleEndian.PutUint16(headerLen, uint16(len(headerBuf)))

	// groupIdx + headerLen + headBuf + body + checksum
	buf := make([]byte, 0, 8+headlen_len+len(headerBuf)+len(body)+checksum_len)

	buf = append(buf, groupIdx...)
	buf = append(buf, headerLen...)
	buf = append(buf, headerBuf...)
	buf = append(buf, body...)

	checksum := crc(0, buf)
	checksumBuf := make([]byte, checksum_len)
	binary.LittleEndian.PutUint32(checksumBuf, checksum)

	buf = append(buf, checksumBuf...)

	return buf
}

func (b base) sendCheckpointMessage(sendToNodeID uint64, checkpointMsg *paxospb.CheckpointMsg, protocol TransportType) error {
	if sendToNodeID == b.conf.getMyNodeID() {
		return nil
	}

	buf, err := b.packCheckpointMsg(checkpointMsg)
	if err != nil {
		return err
	}

	return b.comm.SendMessage(sendToNodeID, buf, protocol)
}

func (b base) sendPaxosMessage(sendToNodeID uint64, paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {
	if b.isTestMode {
		return nil
	}

	getBPInstance().SendMessage()

	if sendToNodeID == b.conf.getMyNodeID() {
		b.instance.onReceivePaxosMsg(paxosMsg, false)
		return nil
	}

	buf, err := b.packMsg(paxosMsg)
	if err != nil {
		return err
	}

	return b.comm.SendMessage(sendToNodeID, buf, protocol)
}

func (b base) broadcastMessage(paxosMsg *paxospb.PaxosMsg, runType broadcastMessage_Type, protocol TransportType) error {
	if b.isTestMode {
		return nil
	}

	getBPInstance().BroadcastMessage()

	if runType == broadcastMessage_Type_RunSelf_First {
		if err := b.instance.onReceivePaxosMsg(paxosMsg, false); err != nil {
			return err
		}
	}

	buf, err := b.packMsg(paxosMsg)
	if err != nil {
		return err
	}
	err = b.comm.BroadcastMessage(buf, protocol)

	if runType == broadcastMessage_Type_RunSelf_Final {
		b.instance.onReceivePaxosMsg(paxosMsg, false)
	}
	return err
}

func (b base) broadcastMessageToFollower(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {
	buf, err := b.packMsg(paxosMsg)
	if err != nil {
		return err
	}

	return b.comm.BroadcastMessageFollower(buf, protocol)
}

func (b base) broadcastMessageToTempNode(paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {
	buf, err := b.packMsg(paxosMsg)
	if err != nil {
		return err
	}

	return b.comm.BroadcastMessageTempNode(buf, protocol)
}

func (b base) setAsTestMode() {
	b.isTestMode = true
}

func unpackBaseMsg(buf []byte) (*paxospb.Header, int, int, error) {
	headerLen := binary.LittleEndian.Uint16(buf[8:10])

	headerStartPos := 10

	bodyStartPos := headerStartPos + int(headerLen)

	if bodyStartPos > len(buf) {
		getBPInstance().UnPackHeaderLenTooLong()

		lNLErr("Header headerlen too long %d", headerLen)
		return nil, 0, 0, errHeaderLenTooLong
	}

	header := &paxospb.Header{}
	err := header.Unmarshal(buf[headerStartPos : headerStartPos+int(headerLen)])
	if err != nil {
		lNLErr("Header.Unmarshal fail, skip this msg")
		return nil, 0, 0, err
	}

	lNLDebug("buffer_size %d header len %d cmdid %d gid %d rid %d version %d body_startpos %d",
		len(buf), headerLen, header.GetCmdid(), header.GetGid(), header.GetRid(), header.GetVersion(), bodyStartPos)

	var bodyLen int
	if header.GetVersion() >= 1 {
		if bodyStartPos+checksum_len > len(buf) {
			lNLErr("no checksum, body start pos %d buffersize %d", bodyStartPos, len(buf))
			return nil, 0, 0, errChecksumMiss
		}

		bodyLen = len(buf) - checksum_len - bodyStartPos

		bufChecksum := binary.LittleEndian.Uint32(buf[len(buf)-checksum_len:])

		checksum := crc(0, buf[:len(buf)-checksum_len])

		if checksum != bufChecksum {
			getBPInstance().UnPackChecksumNotSame()
			lNLErr("Data.bring.checksum %d not equal to Data.cal.checksum %d",
				bufChecksum, checksum)
			return nil, 0, 0, errChecksumNotMatch
		}
	} else {
		bodyLen = len(buf) - bodyStartPos
	}

	return header, bodyStartPos, bodyLen, nil
}

///////////////////////////////////////////////////////////////////////////////
type msgCounter struct {
	conf                        *config
	receiveMsgNodeIDSet         map[uint64]struct{}
	rejectMsgNodeIDSet          map[uint64]struct{}
	promiseOrAcceptMsgNodeIDSet map[uint64]struct{}
}

func newMsgCounter(conf *config) *msgCounter {
	ret := &msgCounter{
		conf: conf,
	}

	ret.startNewRound()
	return ret
}

func (m *msgCounter) startNewRound() {
	m.receiveMsgNodeIDSet = make(map[uint64]struct{})
	m.rejectMsgNodeIDSet = make(map[uint64]struct{})
	m.promiseOrAcceptMsgNodeIDSet = make(map[uint64]struct{})
}

func (m *msgCounter) addReceive(id uint64) {
	if _, ok := m.receiveMsgNodeIDSet[id]; !ok {
		m.receiveMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addReject(id uint64) {
	if _, ok := m.rejectMsgNodeIDSet[id]; !ok {
		m.rejectMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addPromiseOrAccept(id uint64) {
	if _, ok := m.promiseOrAcceptMsgNodeIDSet[id]; !ok {
		m.promiseOrAcceptMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) isPassedOnThisRound() bool {
	return len(m.promiseOrAcceptMsgNodeIDSet) >= m.conf.getMajorityCount()
}

func (m *msgCounter) isRejectedOnThisRound() bool {
	return len(m.rejectMsgNodeIDSet) >= m.conf.getMajorityCount()
}

func (m *msgCounter) isAllReceiveOnThisRound() bool {
	return len(m.receiveMsgNodeIDSet) >= m.conf.getMajorityCount()
}
