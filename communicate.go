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
	SendMessage(toNodeID uint64, msg []byte, typ TransportType) error
	BroadcastMessage(msg []byte, typ TransportType) error
	BroadcastMessageFollower(msg []byte, typ TransportType) error
	BroadcastMessageTempNode(msg []byte, typ TransportType) error
}

// communicate implements the interface MsgTransport.
type communicate struct {
	conf       *config
	network    Network
	myNodeID   uint64
	maxUDPSize uint64
}

func newCommunicate(conf *config, id uint64, maxUDPSize uint64, network Network) *communicate {
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

	if uint64(len(msg)) > c.maxUDPSize || typ == TCP {
		getBPInstance().SendTcp(string(msg))
		return c.network.SendMessageTCP(info.GetIP(), info.GetPort(), msg)
	}

	getBPInstance().SendUdp(string(msg))
	return c.network.SendMessageUDP(info.GetIP(), info.GetPort(), msg)
}

func (c *communicate) SendMessage(id uint64, msg []byte, typ TransportType) error {
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

	lPLGDebug(c.conf.groupIdx, "%d node", len(followers))

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

	lPLGDebug(c.conf.groupIdx, "%d node", len(tmpNodes))

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

///////////////////////////////////////////////////////////////////////////////

type transport interface {
	init(ip string, port int) error
	run()
	stop()
	addMessage(ip string, port int, msg []byte) error
	onReceiveMessage(msg []byte) error
}

// tcpTransport implements the transport interface
type tcpTransport struct {
	listener      net.Listener
	network       *dfNetwork
	isRecvEnd     bool
	isRecvStarted bool
	queue         chan *queueData
	isSendEnd     bool
	isSendStarted bool
}

func newTCPTransport(network *dfNetwork) *tcpTransport {
	return &tcpTransport{
		network: network,
		queue:   make(chan *queueData, getInsideOptionsInstance().getMaxQueueLen()),
	}
}

func (t *tcpTransport) init(ip string, port int) error {
	var err error
	t.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return err
	}

	return nil
}

func (t *tcpTransport) run() {
	go t.sendLoop()
	go t.recvLoop()
}

func (t *tcpTransport) recvLoop() {
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

		if n > 0 {
			t.onReceiveMessage(buf[:n])
		}
	}
}

func (t *tcpTransport) sendLoop() {
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

func (t *tcpTransport) stop() {
	if t.listener != nil {
		t.listener.Close()
		t.listener = nil
	}

	if t.isRecvStarted {
		t.isRecvEnd = true
	}

	if t.isSendStarted {
		t.isSendEnd = true
	}
}

func (t *tcpTransport) addMessage(ip string, port int, msg []byte) error {
	// channel is full
	if len(t.queue) == getInsideOptionsInstance().getMaxQueueLen() {
		return errMsgQueueFull
	}

	t.queue <- newQueueData(ip, port, msg)

	return nil
}

func (t *tcpTransport) sendMessage(ip string, port int, msg []byte) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), time.Second*3)
	if err != nil {
		lPLErr("dial to remote error: %v", err)
		return
	}
	_, err = conn.Write(msg)
	if err != nil {
		lPLErr("write from tcp error: %v", err)
	}
}

func (t *tcpTransport) onReceiveMessage(msg []byte) error {
	return t.network.OnReceiveMessage(msg)
}

// udpTransport implements the transport interface
type udpTransport struct {
	network       *dfNetwork
	isRecvEnd     bool
	isRecvStarted bool
	queue         chan *queueData
	conn          *net.UDPConn
	isSendEnd     bool
	isSendStarted bool
}

func newUDPTransport(network *dfNetwork) *udpTransport {
	return &udpTransport{
		network: network,
		queue:   make(chan *queueData, getInsideOptionsInstance().getMaxQueueLen()),
	}
}

func (t *udpTransport) init(ip string, port int) error {
	var err error
	addr := fmt.Sprintf("%s:%d", ip, port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	t.conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}

	return nil
}

func (t *udpTransport) run() {
	go t.sendLoop()
	go t.recvLoop()
}

func (t *udpTransport) recvLoop() {
	t.isRecvStarted = true
	buf := make([]byte, 65536)
	for {
		if t.isRecvEnd {
			lPLHead("UDPRecv [END]")
			return
		}

		n, _, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			lPLErr("read from udp error: %v", err)
			continue
		}

		getBPInstance().UDPReceive(n)

		if n > 0 {
			t.onReceiveMessage(buf[:n])
		}
	}
}

func (t *udpTransport) sendLoop() {
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

func (t *udpTransport) stop() {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}

	if t.isRecvStarted {
		t.isRecvEnd = true
	}

	if t.isSendStarted {
		t.isSendEnd = true
	}
}

func (t *udpTransport) addMessage(ip string, port int, msg []byte) error {
	// channel is full
	if len(t.queue) == getInsideOptionsInstance().getMaxQueueLen() {
		getBPInstance().UDPQueueFull()
		return errMsgQueueFull
	}

	t.queue <- newQueueData(ip, port, msg)

	return nil
}

func (t *udpTransport) sendMessage(ip string, port int, msg []byte) {
	conn, err := net.DialTimeout("udp", fmt.Sprintf("%s:%d", ip, port), time.Second*3)
	if err != nil {
		lPLErr("dial to remote error: %v", err)
		return
	}
	n, _ := conn.Write(msg)
	if n > 0 {
		getBPInstance().UDPRealSend(string(msg))
	}
}

func (t *udpTransport) onReceiveMessage(msg []byte) error {
	return t.network.OnReceiveMessage(msg)
}
