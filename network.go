package gopaxos

type Network interface {
	RunNetWork()
	StopNetWork()
	SendMessageTCP(ip string, port int, msg []byte) error
	SendMessageUDP(ip string, port int, msg []byte) error
	OnReceiveMessage(msg []byte) error
}

type dfNetwork struct {
	n            Node
	tcpTransport transport
	udpTransport transport
}

func newDFNetwork(n Node) *dfNetwork {
	ret := &dfNetwork{}

	ret.n = n
	ret.tcpTransport = newTCPTransport(ret)
	ret.udpTransport = newUDPTransport(ret)

	return ret
}

func (d *dfNetwork) Init(ip string, port int) error {
	err := d.udpTransport.init(ip, port)
	if err != nil {
		lPLErr("udp transport init fail, error: %v", err)
		return err
	}

	err = d.tcpTransport.init(ip, port)
	if err != nil {
		lPLErr("tcp transport init fail, error: %v", err)
		return err
	}

	return nil
}

func (d *dfNetwork) RunNetWork() {
	d.udpTransport.run()
	d.tcpTransport.run()
}

func (d *dfNetwork) StopNetWork() {
	d.udpTransport.stop()
	d.tcpTransport.stop()
}

func (d *dfNetwork) SendMessageTCP(ip string, port int, msg []byte) error {
	return d.tcpTransport.addMessage(ip, port, msg)
}

func (d *dfNetwork) SendMessageUDP(ip string, port int, msg []byte) error {
	return d.udpTransport.addMessage(ip, port, msg)
}

func (d *dfNetwork) OnReceiveMessage(msg []byte) error {
	if d.n != nil {
		return d.n.OnReceiveMessage(msg)
	}
	lPLHead("receive msglen %d", len(msg))
	return nil
}
