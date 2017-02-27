package gopaxos

import (
	"encoding/binary"
	"github.com/buptmiao/gopaxos/paxospb"
	"hash/crc32"
)

type ballotNumber struct {
	proposalID uint64
	nodeID     nodeId
}

func newBallotNumber(pid uint64, nid nodeId) *ballotNumber {
	return &ballotNumber{
		proposalID: pid,
		nodeID:     nid,
	}
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

func (b base) newInstance() {
	b.instanceID++
	b.initForNewPaxosInstance()
}

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

func (b base) packBaseMsg(body []byte, cmd int32) []byte {
	groupIdx := make([]byte, 8)
	binary.LittleEndian.PutUint64(groupIdx, uint64(b.conf.groupIdx))

	header := &paxospb.Header{}
	header.Gid = b.conf.getGid()
	header.Rid = 0
	header.Cmdid = cmd
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

	checksum := crc32.ChecksumIEEE(buf)
	checksumBuf := make([]byte, checksum_len)
	binary.LittleEndian.PutUint32(checksumBuf, checksum)

	buf = append(buf, checksumBuf...)

	return buf
}

func (b base) sendCheckpointMessage(sendToNodeID nodeId, checkpointMsg *paxospb.CheckpointMsg, protocol TransportType) error {
	if sendToNodeID == b.conf.getMyNodeID() {
		return nil
	}

	buf, err := b.packCheckpointMsg(checkpointMsg)
	if err != nil {
		return err
	}

	return b.comm.SendMessage(sendToNodeID, buf, protocol)
}

func (b base) sendPaxosMessage(sendToNodeID nodeId, paxosMsg *paxospb.PaxosMsg, protocol TransportType) error {
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

func (b base) broadcastMessage(paxosMsg *paxospb.PaxosMsg, runType int, protocol TransportType) error {
	if b.isTestMode {
		return 0
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

	bodyStartPos := headerStartPos + headerLen

	if bodyStartPos > len(buf) {
		getBPInstance().UnPackHeaderLenTooLong()

		lNLErr("Header headerlen too long %d", headerLen)
		return nil, 0, 0, errHeaderLenTooLong
	}

	header := paxospb.Header{}
	err := header.Unmarshal(buf[headerStartPos : headerStartPos+headerLen])
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

		checksum := crc32.ChecksumIEEE(buf[:len(buf)-checksum_len])

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
	receiveMsgNodeIDSet         map[nodeId]struct{}
	rejectMsgNodeIDSet          map[nodeId]struct{}
	promiseOrAcceptMsgNodeIDSet map[nodeId]struct{}
}

func newMsgCounter(conf *config) *msgCounter {
	ret := &msgCounter{
		conf: conf,
	}

	ret.startNewRound()
	return ret
}

func (m *msgCounter) startNewRound() {
	m.receiveMsgNodeIDSet = make(map[nodeId]struct{})
	m.rejectMsgNodeIDSet = make(map[nodeId]struct{})
	m.promiseOrAcceptMsgNodeIDSet = make(map[nodeId]struct{})
}

func (m *msgCounter) addReceive(id nodeId) {
	if _, ok := m.receiveMsgNodeIDSet[id]; !ok {
		m.receiveMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addReject(id nodeId) {
	if _, ok := m.rejectMsgNodeIDSet[id]; !ok {
		m.rejectMsgNodeIDSet[id] = struct{}{}
	}
}

func (m *msgCounter) addPromiseOrAccept(id nodeId) {
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
