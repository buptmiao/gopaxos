package gopaxos

import (
	"fmt"
	"net"
	"time"
)

type TransportType string

const (
	UDP TransportType = "udp"
	TCP TransportType = "tcp"
)

type MsgTransport interface {
	SendMessage(toNodeID nodeId, msg []byte, typ TransportType) error
	BroadcastMessage(msg []byte, typ TransportType) error
	BroadcastMessageFollower(msg []byte, typ TransportType) error
	BroadcastMessageTempNode(msg []byte, typ TransportType) error
}

// communicate implements the interface MsgTransport.
type communicate struct {
	conf       *config
	network    Network
	myNodeID   nodeId
	maxUDPSize uint64
}

func newCommunicate(conf *config, id nodeId, maxUDPSize uint64, network Network) *communicate {
	return &communicate{
		conf:       conf,
		network:    network,
		myNodeID:   id,
		maxUDPSize: maxUDPSize,
	}
}

func (c *communicate) send(info *NodeInfo, msg []byte, typ TransportType) error {
	if len(msg) > getInsideOptionsInstance().getMaxBufferSize() {
		getBPInstance().SendRejectByTooLargeSize()
		lPLGErr(c.conf.groupIdx, "Message size too large %d, max size %d, skip message",
			len(msg), getInsideOptionsInstance().getMaxBufferSize())
		return nil
	}

	getBPInstance().Send(string(msg))

	if len(msg) > c.maxUDPSize || typ == TCP {
		getBPInstance().SendTcp(string(msg))
		return c.network.SendMessageTCP(info.GetIP(), info.GetPort(), msg)
	}

	getBPInstance().SendUdp(string(msg))
	return c.network.SendMessageUDP(info.GetIP(), info.GetPort(), msg)
}

func (c *communicate) SendMessage(id nodeId, msg []byte, typ TransportType) error {
	n := NewNodeInfo(id, "", 0)
	n.parseNodeID()
	return c.send(n, msg, typ)
}

func (c *communicate) BroadcastMessage(msg []byte, typ TransportType) error {
	members := c.conf.getSystemVSM().getMembershipMap()

	for k, _ := range members {
		if k != c.myNodeID {
			nodeInfo := NewNodeInfo(k, "", 0)
			nodeInfo.parseNodeID()
			c.send(nodeInfo, msg, typ)
		}
	}

	return nil
}

func (c *communicate) BroadcastMessageFollower(msg []byte, typ TransportType) error {
	followers := c.conf.getMyFollowerMap()

	for k, _ := range followers {
		if k != c.myNodeID {
			nodeInfo := NewNodeInfo(k, "", 0)
			nodeInfo.parseNodeID()
			c.send(nodeInfo, msg, typ)
		}
	}

	lPLGDebug("%d node", len(followers))

	return nil
}

func (c *communicate) BroadcastMessageTempNode(msg []byte, typ TransportType) error {
	tmpNodes := c.conf.getTmpNodeMap()

	for k, _ := range tmpNodes {
		if k != c.myNodeID {
			nodeInfo := NewNodeInfo(k, "", 0)
			nodeInfo.parseNodeID()
			c.send(nodeInfo, msg, typ)
		}
	}

	lPLGDebug("%d node", len(tmpNodes))

	return nil
}

func (c *communicate) SetUDPMaxSize(size uint64) {
	c.maxUDPSize = size
}

type queueData struct {
	ip   string
	port int
	msg  []byte
}

func newQueueData(ip string, port int, msg []byte) *queueData {
	return &queueData{
		ip:   ip,
		port: port,
		msg:  msg,
	}
}

type transport struct {
	protocol      TransportType
	listener      net.Listener
	network       *dfNetwork
	isRecvEnd     bool
	isRecvStarted bool

	queue chan *queueData

	sendConn      net.Conn
	isSendEnd     bool
	isSendStarted bool
}

func newTransport(protocol TransportType, network *dfNetwork) *transport {
	return &transport{
		protocol: protocol,
		network:  network,
		queue:    make(chan *queueData, getInsideOptionsInstance().getMaxQueueLen()),
	}
}

func (t *transport) init(port int) error {
	var err error
	t.listener, err = net.Listen(t.protocol, fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return err
	}

	return nil
}

func (t *transport) run() {
	go t.sendLoop()
	go t.recvLoop()
}

func (t *transport) recvLoop() {
	t.isRecvStarted = true
	buf := make([]byte, 65536)
	for {
		if t.isRecvEnd {
			lPLHead("UDPRecv [END]")
			return
		}

		conn, err := t.listener.Accept()
		if err != nil {
			lPLErr("accept connect error")
			continue
		}

		n, _ := conn.Read(buf)

		getBPInstance().UDPReceive(n)

		if n > 0 {
			t.network.OnReceiveMessage(buf[:n])
		}
	}
}

func (t *transport) sendLoop() {
	t.isSendStarted = true
	for {
		data := <-t.queue
		if data != nil {
			t.sendMessage(data.ip, data.port, data.msg)
		}

		if t.isSendEnd {
			lPLHead("UDPSend [END]")
			return
		}
	}
}

func (t *transport) stop() {
	if t.listener != nil {
		t.listener.Close()
		t.listener = nil
	}

	if t.sendConn != nil {
		t.sendConn.Close()
		t.sendConn = nil
	}

	if t.isRecvStarted {
		t.isRecvEnd = true
	}

	if t.isSendStarted {
		t.isSendEnd = true
	}
}

func (t *transport) addMessage(ip string, port int, msg []byte) error {
	// channel is full
	if len(t.queue) == getInsideOptionsInstance().getMaxQueueLen() {
		getBPInstance().UDPQueueFull()
		return errMsgQueueFull
	}

	t.queue <- newQueueData(ip, port, msg)

	return nil
}

func (t *transport) sendMessage(ip string, port int, msg []byte) {
	conn, err := net.DialTimeout(t.protocol, fmt.Sprintf("%s:%d", ip, port), time.Second*3)
	if err != nil {
		lPLErr("dial to remote error: %v", err)
	}
	n, _ := conn.Write(msg)
	if n > 0 {
		getBPInstance().UDPRealSend(string(msg))
	}
}
