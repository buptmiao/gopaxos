package gopaxos

type commitCtx struct {
	commitValue []byte
}

func newCommitCtx(conf *config) *commitCtx {

}

func (c *commitCtx) isNewCommit() bool {

}

func (c *commitCtx) setResultOnlyRet(commitRet int) {

}

func (c *commitCtx) setResult(commitRet int, instanceID uint64, learnValue []byte) {

}

func (c *commitCtx) getCommitValue() []byte {
	return c.commitValue
}

func (c *commitCtx) setCommitValue(value []byte) {
	c.commitValue = value
}

func (c *commitCtx) startCommit(instanceID uint64) {

}

func (c *commitCtx) getTimeoutMs() int {

}

func (c *commitCtx) isMyCommit(instanceID uint64, learnValue []byte) (*SMCtx, bool) {

}
