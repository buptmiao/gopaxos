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

func (d *dfNetwork) Init(ip string, port int) error {

}

func (d *dfNetwork) RunNetWork() {

}

func (d *dfNetwork) StopNetWork() {
	d.udpTransport.stop()
	d.tcpTransport.stop()
}

func (d *dfNetwork) SendMessageTCP(ip string, port int, msg []byte) error {

}

func (d *dfNetwork) SendMessageUDP(ip string, port int, msg []byte) error {

}

func (d *dfNetwork) OnReceiveMessage(msg []byte) error {
	if d.n != nil {
		return d.n.OnReceiveMessage(msg)
	}
	lPLHead("receive msglen %d", len(msg))
	return nil
}
