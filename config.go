package gopaxos

type config struct {
	logSync       bool
	syncInterval  int
	useMembership bool

	myNodeID   nodeId
	nodeCount  int
	groupIdx   int
	groupCount int

	nodeInfoList        *NodeInfoList
	isFollower          bool
	followToNodeID      nodeId
	sysVSM              *systemVSM
	masterSM            insideSM
	tmpNodeOnlyForLearn map[nodeId]uint64
	myFollower          map[nodeId]uint64
}

func newConfig(ls LogStorage, logSync bool, syncInterval int, useMembership bool,
	n *NodeInfo, nodeList NodeInfoList, followerList FollowerNodeInfoList, groupIdx int,
	groupCount int, cb MembershipChangeCallback) {

	c := &config{}
	c.logSync = logSync
	c.syncInterval = syncInterval
	c.useMembership = useMembership
	c.myNodeID = n.GetNodeID()
	c.nodeCount = len(nodeList)
	c.groupIdx = groupIdx
	c.groupCount = groupCount
	c.sysVSM = newSystemVSM(groupIdx, n.GetNodeID(), ls, cb)
	c.masterSM = nil

	c.nodeInfoList = nodeList
	c.isIMFollower = false
	c.followToNodeID = nullNode

	for _, follower := range followerList {
		if follower.MyNode.GetNodeID() == n.GetNodeID() {
			lPLG1Head("I'm follower, ip %s port %d nodeid %d",
				n.GetIP(), n.GetPort(), n.GetNodeID())
			c.isFollower = true
			c.followToNodeID = follower.FollowNode.GetNodeID()

			getInsideOptionsInstance().setAsFollower()
		}
	}
	return c
}

func (c *config) init() error {
	err := c.sysVSM.init()
	if err != nil {
		lPLG1Err("fail, ret %v", err)
		return err
	}

	c.sysVSM.addNodeIDList(c.nodeInfoList)

	lPLG1Head(c.groupIdx, "OK")
	return nil
}

func (c *config) checkConfig() bool {
	if !c.sysVSM.isIMInMembership() {
		lPLG1Err("my node %d is not in membership", c.myNodeID)
		return false
	}

	return true
}

func (c *config) getGid() uint64 {
	return c.sysVSM.getGid()
}

func (c *config) getMyNodeID() nodeId {
	return c.myNodeID
}

func (c *config) getNodeCount() int {
	return c.sysVSM.getNodeCount()
}

func (c *config) getMyGroupIdx() int {
	return c.groupIdx
}

func (c *config) getGroupCount() int {
	return c.groupCount
}

func (c *config) getMajorityCount() int {
	return c.sysVSM.getMajorityCount()
}

func (c *config) getIsUseMembership() bool {
	return c.useMembership
}

func (c *config) getAskForLearnTimeoutMs() uint64 {
	return 2000
}

func (c *config) getPrepareTimeoutMs() int {
	return 3000
}

func (c *config) getAcceptTimeoutMs() int {
	return 3000
}

func (c *config) isValidNodeID(id nodeId) bool {
	return c.sysVSM.isValidNodeID(id)
}

func (c *config) isIMFollower() bool {
	return c.isFollower
}

func (c *config) getFollowToNodeID() nodeId {
	return c.followToNodeID
}

func (c *config) getSystemVSM() *systemVSM {
	return c.sysVSM
}

func (c *config) setMasterSM(sm insideSM) {
	c.masterSM = sm
}

func (c *config) getMasterSM() insideSM {
	return c.masterSM
}

func (c *config) addTmpNodeOnlyForLearn(id nodeId) {
	nodeIDSet := c.sysVSM.getMembershipMap()
	_, ok := nodeIDSet[id]
	if ok {
		return
	}

	c.tmpNodeOnlyForLearn[id] = getSteadyClockMS() + 60000
}

func (c *config) getTmpNodeMap() map[nodeId]uint64 {
	now := getSteadyClockMS()

	tmp := make(map[nodeId]uint64)
	for k, v := range c.tmpNodeOnlyForLearn {
		if v < now {
			lPLErr("tmpnode %d timeout, nowtimems %d tmpnode last add time %d",
				k, now, v)
		} else {
			tmp[k] = v
		}
	}

	c.tmpNodeOnlyForLearn = tmp

	return tmp
}

func (c *config) addFollowerNode(myFollowerNodeID nodeId) {
	c.myFollower[myFollowerNodeID] = getSteadyClockMS() + getInsideOptionsInstance().getAskforLearnInterval()*3
}

func (c *config) getMyFollowerMap() map[nodeId]uint64 {
	now := getSteadyClockMS()

	tmp := make(map[nodeId]uint64)
	for k, v := range c.myFollower {
		if v < now {
			lPLErr("follower %d timeout, nowtimems %d tmpnode last add time %d",
				k, now, v)
		} else {
			tmp[k] = v
		}
	}

	c.myFollower = tmp

	return tmp
}

func (c *config) getMyFollowerCount() int {
	return len(c.myFollower)
}

func (c *config) getLogSync() bool {
	return c.logSync
}

func (c *config) setLogSync(logSync bool) {
	c.logSync = logSync
}

func (c *config) getSyncInterval() int {
	return c.syncInterval
}
