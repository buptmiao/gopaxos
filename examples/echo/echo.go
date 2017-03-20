package main

import (
	"flag"
	"fmt"
	"github.com/buptmiao/gopaxos"
	"github.com/pkg/errors"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	Log *log.Logger
)

func init() {
	format := log.Ldate | log.Ltime | log.Lshortfile
	Log = log.New(os.Stdout, "[INFO]: ", format)
}

type echoSMCtx struct {
	executeRet    int
	echoRespValue []byte
}

type echoSM struct{}

func (e *echoSM) SMID() int64 {
	return 1
}

func (e *echoSM) Execute(groupIdx int, instanceID uint64, paxosValue []byte, smCtx *gopaxos.SMCtx) bool {
	fmt.Printf("[SM Execute] ok, smid %d instanceid %d value %s\n",
		e.SMID(), instanceID, string(paxosValue))

	if smCtx != nil && smCtx.Ctx != nil {
		ctx := smCtx.Ctx.(*echoSMCtx)
		ctx.executeRet = 0
		ctx.echoRespValue = paxosValue
	}

	return true
}

func (e *echoSM) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue []byte) bool {
	return true
}

func (e *echoSM) GetCheckpointInstanceID(groupIdx int) uint64 {
	return 0
}

func (e *echoSM) LockCheckpointState() error {
	return nil
}

func (e *echoSM) GetCheckpointState(groupIdx int) (string, []string, error) {
	return "", nil, nil
}

func (e *echoSM) UnLockCheckpointState() {
	return
}

func (e *echoSM) LoadCheckpointState(groupIdx int, checkpointTmpFileDirPath string, fileList []string, checkpointInstanceID uint64) error {
	return nil
}

func (e *echoSM) BeforePropose(groupIdx int, value []byte) []byte {
	return nil
}

func (e *echoSM) NeedCallBeforePropose() bool {
	return true
}

///////////////////////////////////////////////////////////////////////////////

type echoServer struct {
	myNode   gopaxos.NodeInfo
	nodeList gopaxos.NodeInfoList
	paxos    gopaxos.Node
	sm       *echoSM
}

func newEchoServer(nodeInfo *gopaxos.NodeInfo, list gopaxos.NodeInfoList) *echoServer {
	return &echoServer{
		myNode:   *nodeInfo,
		nodeList: list,
		paxos:    nil,
	}
}

func (e *echoServer) makeLogStoragePath() (string, error) {
	logPath := fmt.Sprintf("./logpath_%s_%d", e.myNode.GetIP(), e.myNode.GetPort())

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		err = os.Mkdir(logPath, 0775)
		if err != nil {
			Log.Printf("Create dir fail, path %s", logPath)
			return "", err
		}
	}

	return logPath, nil
}

func (e *echoServer) RunPaxos() error {
	opt := gopaxos.NewOptions()

	var err error
	opt.LogStoragePath, err = e.makeLogStoragePath()
	if err != nil {
		return err
	}

	opt.GroupCount = 1
	opt.MyNode = e.myNode
	opt.NodeInfoList = e.nodeList
	//opt.LogLevel = gopaxos.LogLevel_Verbose

	smInfo := gopaxos.NewGroupInfo()

	smInfo.GroupIdx = 0
	smInfo.SMList = append(smInfo.SMList, e.sm)

	opt.GroupSMInfoList = append(opt.GroupSMInfoList, smInfo)

	e.paxos, err = gopaxos.RunNode(opt)
	if err != nil {
		Log.Printf("run paxos fail, error: %v\n", err)
		return err
	}

	Log.Printf("run paxos ok\n")

	return nil
}

func (e *echoServer) Echo(value []byte) ([]byte, error) {
	ctx := &gopaxos.SMCtx{}

	echosmCtx := &echoSMCtx{}

	ctx.SMID = 1
	ctx.Ctx = echosmCtx

	var err error
	_, err = e.paxos.Propose(0, value, ctx)
	if err != nil {
		Log.Printf("paxos propose fail, error: %v\n", err)
		return nil, err
	}

	if echosmCtx.executeRet != 0 {
		Log.Printf("echo sm excute fail, excuteret %d\n", echosmCtx.executeRet)
		return nil, errors.New("execute fail")
	}

	resp := echosmCtx.echoRespValue

	return resp, nil
}

func parseIpPort(myNodeStr string) *gopaxos.NodeInfo {
	myNode := gopaxos.NewNodeInfo(0, "", 0)
	var ip string
	var port int
	ss := strings.Split(myNodeStr, ":")
	if len(ss) != 2 {
		Log.Fatalf("input addr %s not valid", myNodeStr)
	}
	ip = ss[0]
	port, err := strconv.Atoi(ss[1])
	if err != nil {
		Log.Fatalf("input addr %s not valid", myNodeStr)
	}
	myNode.SetIPPort(ip, port)
	return myNode
}

func parseIpPortList(nodeListStr string) gopaxos.NodeInfoList {
	ss := strings.Split(nodeListStr, ",")

	if len(ss) < 1 {
		Log.Fatalf("input node list %s is illegal", nodeListStr)
	}
	ret := make(gopaxos.NodeInfoList, 0, len(ss))
	for _, node := range ss {
		ret = append(ret, parseIpPort(node))
	}

	return ret
}

func main() {
	nodeListStr := flag.String("members", "", "cluster members ip:port list")
	myNodeStr := flag.String("addr", ":15000", "local ip:port")
	flag.Parse()

	myNode := parseIpPort(*myNodeStr)
	nodeList := parseIpPortList(*nodeListStr)

	echoSvr := newEchoServer(myNode, nodeList)

	err := echoSvr.RunPaxos()
	if err != nil {
		return
	}

	Log.Printf("echo server start, ip %s port %d\n", myNode.GetIP(), myNode.GetPort())

	for {
		var input string
		fmt.Scanln(&input)

		Log.Printf("input: %s\n", input)

		resp, err := echoSvr.Echo([]byte(input))
		if err != nil {
			Log.Printf("Echo fail, error: %v\n", err)
		} else {
			Log.Printf("echo resp value %s\n", string(resp))
		}
	}

	return
}
