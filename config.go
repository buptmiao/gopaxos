package gopaxos

type config struct {
	logSync       bool
	syncInterval  int
	useMembership bool

	myNodeID   uint64
	nodeCount  int
	groupIdx   int
	groupCount int

	nodeInfoList        NodeInfoList
	isFollower          bool
	followToNodeID      uint64
	sysVSM              *systemVSM
	masterSM            insideSM
	tmpNodeOnlyForLearn map[uint64]uint64
	myFollower          map[uint64]uint64
}

func newConfig(ls LogStorage, logSync bool, syncInterval int, useMembership bool,
	n *NodeInfo, nodeList NodeInfoList, followerList FollowerNodeInfoList, groupIdx int,
	groupCount int, cb MembershipChangeCallback) *config {

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
	c.isFollower = false
	c.followToNodeID = nullNode

	for _, follower := range followerList {
		if follower.MyNode.GetNodeID() == n.GetNodeID() {
			lPLGHead(groupIdx, "I'm follower, ip %s port %d nodeid %d",
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
		lPLGErr(c.groupIdx, "fail, error: %v", err)
		return err
	}

	c.sysVSM.addNodeIDList(c.nodeInfoList)

	lPLGHead(c.groupIdx, "OK")
	return nil
}

func (c *config) checkConfig() bool {
	if !c.sysVSM.isIMInMembership() {
		lPLGErr(c.groupIdx, "my node %d is not in membership", c.myNodeID)
		return false
	}

	return true
}

func (c *config) getGid() uint64 {
	return c.sysVSM.getGid()
}

func (c *config) getMyNodeID() uint64 {
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

func (c *config) isValidNodeID(id uint64) bool {
	return c.sysVSM.isValidNodeID(id)
}

func (c *config) isIMFollower() bool {
	return c.isFollower
}

func (c *config) getFollowToNodeID() uint64 {
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

func (c *config) addTmpNodeOnlyForLearn(id uint64) {
	nodeIDSet := c.sysVSM.getMembershipMap()
	_, ok := nodeIDSet[id]
	if ok {
		return
	}

	c.tmpNodeOnlyForLearn[id] = getSteadyClockMS() + 60000
}

func (c *config) getTmpNodeMap() map[uint64]uint64 {
	now := getSteadyClockMS()

	tmp := make(map[uint64]uint64)
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

func (c *config) addFollowerNode(myFollowerNodeID uint64) {
	c.myFollower[myFollowerNodeID] = getSteadyClockMS() + uint64(getInsideOptionsInstance().getAskforLearnInterval()*3)
}

func (c *config) getMyFollowerMap() map[uint64]uint64 {
	now := getSteadyClockMS()

	tmp := make(map[uint64]uint64)
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
