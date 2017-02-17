package paxos

import (
	"encoding/binary"
	"net"
)

// Options is the global config of paxos.
type Options struct {
	//optional
	//User-specified paxoslog storage.
	//Default is nil
	LogStorage LogStorage

	//optional
	//If poLogStorage == nil, sLogStoragePath is required.
	LogStoragePath string

	//optional
	//If true, the write will be flushed from the operating system
	//buffer cache before the write is considered complete.
	//If this flag is true, writes will be slower.
	//
	//If this flag is false, and the machine crashes, some recent
	//writes may be lost. Note that if it is just the process that
	//crashes (i.e., the machine does not reboot), no writes will be
	//lost even if sync==false. Because of the data lost, we not guarantee consistence.
	//
	//Default is true.
	Sync bool

	//optional
	//Default is 0.
	//This means the write will skip flush at most iSyncInterval times.
	//That also means you will lost at most iSyncInterval count's paxos log.
	SyncInterval int

	//optional
	//User-specified network.
	Network Network

	//optional
	//Our default network use udp and tcp combination, a message we use udp or tcp to send decide by a threshold.
	//Message size under iUDPMaxSize we use udp to send.
	//Default is 4096.
	UDPMaxSize uint64

	//optional
	//We support to run multi phxpaxos on one process.
	//One paxos group here means one independent phxpaxos. Any two phxpaxos(paxos group) only share network, no other.
	//There is no communication between any two paxos group.
	//Default is 1.
	GroupCount int

	//required
	//Self node's ip/port.
	MyNode NodeInfo

	//required
	//All nodes's ip/port with a paxos set(usually three or five nodes).
	NodeInfoList NodeInfoList

	//optional
	//Only bUseMembership == true, we use option's sliNodeInfoList to init paxos membership,
	//after that, paxos will remember all nodeinfos, so second time you can run paxos without sliNodeInfoList,
	//and you can only change membership by the api.
	//
	//Default is false.
	//if bUseMembership == false, that means every time you run paxos will use vecNodeList to build a new membership.
	//when you change membership by a new vecNodeList, we don't guarantee consistence.
	//
	//For test, you can set false.
	//But when you use it to real services, remember to set true.
	UseMembership bool

	//While membership change, phxpaxos will call this function.
	//Default is nil.
	MembershipChangeCallback MembershipChangeCallback

	//optional
	//One phxpaxos can mounting multi state machines.
	//This vector include different phxpaxos's state machines list.
	GroupSMInfoList GroupSMInfoList

	//optional
	Breakpoint Breakpoint

	//optional
	//If use this mode, that means you propose large value(maybe large than 5M means large) much more.
	//Large value means long latency, long timeout, this mode will fit it.
	//Default is false
	IsLargeValueMode bool

	//optional
	//All followers's ip/port, and follow to node's ip/port.
	//Follower only learn but not participation paxos algorithmic process.
	//Default is empty.
	FollowerNodeInfoList FollowerNodeInfoList

	//optional
	//Notice, this function must be thread safe!
	//if pLogFunc == nil, we will print log to standard ouput.
	LogFunc LogFunc

	//optional
	//If you use your own log function, then you control loglevel yourself, ignore this.
	//Check log.go to find 5 level.
	//Default is LogLevel::LogLevel_None, that means print no log.
	LogLevel LogLevel

	//optional
	//If you use checkpoint replayer feature, set as true.
	//Default is false;
	IsUseCheckpointRePlayer bool

	//optional
	//Only bUseBatchPropose is true can use API BatchPropose in node.h
	//Default is false;
	IsUseBatchPropose bool

	//optional
	//Only bOpenChangeValueBeforePropose is true, that will callback sm's function(BeforePropose).
	//Default is false;
	IsOpenChangeValueBeforePropose bool
}

func NewOptions() *Options {
	return &Options{
		LogStorage:                     nil,
		LogStoragePath:                 "",
		Sync:                           true,
		SyncInterval:                   0,
		Network:                        nil,
		UDPMaxSize:                     4096,
		GroupCount:                     1,
		UseMembership:                  false,
		MembershipChangeCallback:       nil,
		Breakpoint:                     nil,
		IsLargeValueMode:               false,
		LogFunc:                        nil,
		LogLevel:                       LogLevel_None,
		IsUseCheckpointRePlayer:        false,
		IsUseBatchPropose:              false,
		IsOpenChangeValueBeforePropose: false,
	}
}

type nodeId uint64

const nullNode = uint64(0)

type NodeInfo struct {
	iNodeId nodeId
	sIP     string
	iPort   int
}

func NewNodeInfo(iNodeId nodeId, ip string, port int) *NodeInfo {
	return &NodeInfo{
		iNodeId: iNodeId,
		sIP:     ip,
		iPort:   port,
	}
}

func (n *NodeInfo) GetNodeID() nodeId {
	return n.iNodeId
}

func (n *NodeInfo) GetIP() string {
	return n.sIP
}

func (n *NodeInfo) GetPort() int {
	return n.iPort
}

func (n *NodeInfo) SetIPPort(ip string, port int) *NodeInfo {
	n.sIP = ip
	n.iPort = port
	n.makeNodeID()
	return n
}

func (n *NodeInfo) SetNodeID(iNodeId nodeId) *NodeInfo {
	n.iNodeId = iNodeId
	n.parseNodeID()
	return n
}

func (n *NodeInfo) makeNodeID() {
	bip := net.ParseIP(n.sIP).To4()
	iip := uint64(binary.LittleEndian.Uint32(bip))
	n.iNodeId = (iip << 32) | n.iPort
}

func (n *NodeInfo) parseNodeID() {
	n.iPort = n.iNodeId & 0xFFFFFFFF
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(n.iNodeId>>32))
	n.sIP = net.IPv4(buf[0], buf[1], buf[2], buf[3]).String()
}

type FollowerNodeInfo struct {
	MyNode     NodeInfo
	FollowNode NodeInfo
}

type NodeInfoList []NodeInfo
type FollowerNodeInfoList []FollowerNodeInfo

type GroupSMInfo struct {
	//required
	//GroupIdx interval is [0, GroupCount)
	GroupIdx int

	//optional
	//One paxos group can mounting multi state machines.
	SMList []StateMachine

	//optional
	//Master election is a internal state machine.
	//Set IsUseMaster as true to open master election feature.
	//Default is false.
	IsUseMaster bool
}

func NewGroupInfo() *GroupSMInfo {
	return &GroupSMInfo{
		GroupIdx:    -1,
		IsUseMaster: false,
	}
}

type GroupSMInfoList []GroupSMInfo
type MembershipChangeCallback func(int, NodeInfoList)
