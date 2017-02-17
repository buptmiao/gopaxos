package paxos

import (
	"os"

	"encoding/binary"
	"fmt"
	"github.com/docker/docker/pkg/random"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"strconv"
	"strings"
)

const (
	minChosenKey       = 1<<64 - 1
	systemVariablesKey = 1<<64 - 2
	masterVariablesKey = 1<<64 - 3
)

var (
	getMinKey uint64 = minChosenKey
	setMinKey uint64 = minChosenKey

	setSystemVariablesKey uint64 = systemVariablesKey
	getSystemVariablesKey uint64 = systemVariablesKey

	setMasterVariablesKey uint64 = masterVariablesKey
	getMasterVariablesKey uint64 = masterVariablesKey
)

//PaxosComparator implements the interface defined in leveldb Comparer
type paxosComparator struct {
}

// db
type db struct {
	levelDB    *leveldb.DB
	cmp        paxosComparator
	hasInit    bool
	valueStore *logStore
	dbPath     string
	groupIdx   int

	timeStat timeStat
}

func newDB() *db {
	return &db{
		groupIdx: -1,
	}
}

func (d *db) clearAllLog() error {
	systemVar, err := d.getSystemVariables()
	if err != nil {
		lPLG1Err(d.groupIdx, "GetSystemVariables fail, ret %v", err)
		return err
	}

	masterVar, err := d.getMasterVariables()
	if err != nil {
		lPLG1Err(d.groupIdx, "GetMasterVariables fail, ret %v", err)
		return err
	}

	d.hasInit = false

	d.levelDB = nil

	d.valueStore = nil

	bakPath := d.dbPath + ".bak"

	err = deleteDir(bakPath)
	if err != nil {
		lPLG1Err(d.groupIdx, "Delete bak dir fail, dir %s", bakPath)
		return err
	}

	err = os.Rename(d.dbPath, bakPath)
	if err != nil {
		panic(err)
	}

	err = d.init(d.dbPath, d.groupIdx)
	if err != nil {
		lPLG1Err(d.groupIdx, "Init again fail, ret %v", err)
		return err
	}

	wo := writeOptions(true)
	if systemVar != "" {
		err = d.setSystemVariables(wo, systemVar)
		if err != nil {
			lPLG1Err(d.groupIdx, "SetSystemVariables fail, ret %v", err)
			return err
		}
	}
	if masterVar != "" {
		err = d.setMasterVariables(wo, masterVar)
		if err != nil {
			lPLG1Err(d.groupIdx, "SetMasterVariables fail, ret %v", err)
			return err
		}
	}

	return nil
}

func (d *db) init(dbPath string, groupIdx int) error {
	if d.hasInit {
		return nil
	}

	d.groupIdx = groupIdx
	d.dbPath = dbPath

	opt := &opt.Options{}
	opt.ErrorIfMissing = false
	opt.Comparer = d.cmp
	opt.WriteBuffer = 1024*1024 + groupIdx*10*1024

	var err error
	d.levelDB, err = leveldb.OpenFile(d.dbPath, opt)
	if err != nil {
		lPLG1Err(d.groupIdx, "Open leveldb fail, db_path %s, err: %v", d.dbPath, err)
		return err
	}

	d.valueStore = newLogStore()

	err = d.valueStore.init(d.dbPath, groupIdx, d)
	if err != nil {
		lPLG1Err(d.groupIdx, "value store init fail, ret %v", err)
		return err
	}

	d.hasInit = true

	lPLG1Imp(d.groupIdx, "OK, db_path %s", d.dbPath)
	return nil
}

func (d *db) getDBPath() string {
	return d.dbPath
}

func (d *db) getMaxInstanceIDFileID() (string, uint64, error) {
	maxInstanceID, err := d.getMaxInstanceID()
	if err != nil && err != errMaxInstanceIDNotExist {
		return "", 0, err
	}

	if err == errMaxInstanceIDNotExist {
		return "", maxInstanceID, nil
	}

	key := d.genKey(maxInstanceID)

	value, err := d.levelDB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			getBPInstance().LevelDBGetNotExist()
			return string(value), 0, ErrNotFoundFromStorage
		}
		getBPInstance().LevelDBGetFail()
		lPLG1Err(d.groupIdx, "LevelDB.Get fail")
		return "", 0, err
	}

	return string(value), maxInstanceID, nil
}

func (d *db) rebuildOneIndex(instanceID uint64, sFileID string) error {
	key := d.genKey(instanceID)

	opt := &opt.WriteOptions{
		Sync: false,
	}

	err := d.levelDB.Put([]byte(key), []byte(sFileID), opt)
	if err != nil {
		getBPInstance().LevelDBPutFail()
		lPLG1Err(d.groupIdx, "LevelDB.Put fail, instanceid %d valuelen %d", instanceID, len(sFileID))
		return err
	}

	return nil
}

func (d *db) getFromLevelDB(instanceID uint64) (string, error) {
	key := d.genKey(instanceID)

	value, err := d.levelDB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			getBPInstance().LevelDBGetNotExist()
			lPLG1Debug(d.groupIdx, "LevelDB.Get not found, instanceid %d", instanceID)
			return "", ErrNotFoundFromStorage
		}
		getBPInstance().LevelDBGetFail()
		lPLG1Err(d.groupIdx, "LevelDB.Get fail, instanceid %d", instanceID)
		return "", err
	}

	return value, nil
}

func (d *db) get(instanceID uint64) (string, error) {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return "", errDBNotInit
	}
	sFileID, err := d.getFromLevelDB(instanceID)
	if err != nil {
		return "", err
	}
	value, fileInstanceID, err := d.fileIDToValue(sFileID)
	if err != nil {
		getBPInstance().FileIDToValueFail()
		return "", err
	}

	if fileInstanceID != instanceID {
		lPLG1Err(d.groupIdx, "file instanceid %d not equal to key.instanceid %d",
			fileInstanceID, instanceID)
		return "", errFileInstanceIDMismatch
	}

	return value, nil
}

func (d *db) valueToFileID(wo writeOptions, instanceID uint64, value []byte) (string, error) {
	sFileID, err := d.valueStore.append(wo, instanceID, value)
	if err != nil {
		getBPInstance().ValueToFileIDFail()
		lPLG1Err(d.groupIdx, "fail, ret %v", err)
	}
	return sFileID, err
}

func (d *db) fileIDToValue(sFileID string) (string, uint64, error) {
	var value string
	instanceID, err := d.valueStore.read(sFileID, &value)
	if err != nil {
		lPLG1Err(d.groupIdx, "fail, ret %v", err)
	}
	return value, instanceID, err
}

func (d *db) putToLevelDB(sync bool, instanceID uint64, value []byte) error {
	key := d.genKey(instanceID)

	opt := &opt.WriteOptions{
		Sync: sync,
	}

	d.timeStat.point()

	err := d.levelDB.Put([]byte(key), value, opt)
	if err != nil {
		getBPInstance().LevelDBPutFail()
		lPLG1Err(d.groupIdx, "LevelDB.Put fail, instanceid %d valuelen %d", instanceID, len(value))
		return err
	}

	getBPInstance().LevelDBPutOK(d.timeStat.point())

	return nil
}

func (d *db) put(wo writeOptions, instanceID uint64, value []byte) error {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return errDBNotInit
	}

	sFileID, err := d.valueToFileID(wo, instanceID, value)
	if err != nil {
		return err
	}

	return d.putToLevelDB(false, instanceID, &sFileID)
}

func (d *db) forceDel(wo writeOptions, instanceID uint64) error {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return errDBNotInit
	}

	key := d.genKey(instanceID)
	value, err := d.levelDB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			lPLG1Debug(d.groupIdx, "LevelDB.Get not found, instanceid %d", instanceID)
			return ErrNotFoundFromStorage
		}

		lPLG1Err(d.groupIdx, "LevelDB.Get fail, instanceid %d", instanceID)
		return err
	}

	sFileID := string(value)
	err = d.valueStore.forceDel(sFileID, instanceID)
	if err != nil {
		return err
	}

	opt := &opt.WriteOptions{
		Sync: wo,
	}
	err = d.levelDB.Delete([]byte(key), opt)
	if err != nil {
		lPLG1Err(d.groupIdx, "LevelDB.Delete fail, instanceid %d", instanceID)
		return err
	}

	return nil
}

func (d *db) del(wo writeOptions, instanceID uint64) error {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return errDBNotInit
	}

	key := d.genKey(instanceID)

	if random.Rand.Int()%100 < 1 {
		value, err := d.levelDB.Get([]byte(key), nil)
		if err != nil {
			if err == leveldb.ErrNotFound {
				lPLG1Debug(d.groupIdx, "LevelDB.Get not found, instanceid %d", instanceID)
				return nil
			}

			lPLG1Err(d.groupIdx, "LevelDB.Get fail, instanceid %d", instanceID)
			return err
		}

		sFileID := string(value)
		if err = d.valueStore.del(sFileID, instanceID); err != nil {
			return err
		}
	}

	opt := &opt.WriteOptions{
		Sync: wo,
	}
	if err := d.levelDB.Delete([]byte(key), opt); err != nil {
		lPLG1Err(d.groupIdx, "LevelDB.Delete fail, instanceid %d", instanceID)
		return err
	}

	return nil
}

//
func (d *db) getMaxInstanceID() (uint64, error) {
	instanceID := minChosenKey
	it := db.levelDB.NewIterator(nil, nil)
	defer it.Release()

	it.Last()

	for it.Valid() {
		instanceID = d.getInstanceIDFromKey(string(it.Key()))
		if instanceID == minChosenKey || instanceID == systemVariablesKey || instanceID == masterVariablesKey {
			it.Prev()
		} else {
			return instanceID, nil
		}
	}

	return instanceID, errMaxInstanceIDNotExist
}

func (d *db) genKey(instanceID uint64) string {
	return strconv.FormatUint(instanceID, 10)
}

func (d *db) getInstanceIDFromKey(key string) uint64 {
	buf := make([]byte, 8)
	copy(buf, key)
	return binary.LittleEndian.Uint64(buf)
}

func (d *db) getMinChosenInstanceID() (uint64, error) {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return 0, errDBNotInit
	}

	value, err := d.getFromLevelDB(getMinKey)
	if err != nil && err != leveldb.ErrNotFound {
		lPLG1Err(d.groupIdx, "fail, ret %v", err)
		return 0, err
	}

	if err == leveldb.ErrNotFound {
		lPLG1Err(d.groupIdx, "no min chosen instanceid")
		return 0, nil
	}

	sFileID := string(value)
	if d.valueStore.isValidFileID(sFileID) {
		value, err = d.get(getMinKey)
		if err != nil && err != leveldb.ErrNotFound {
			lPLG1Err(d.groupIdx, "Get from log store fail, ret %v", err)
			return 0, err
		}
	}

	if len(value) != 8 {
		lPLG1Err(d.groupIdx, "fail, mininstanceid size wrong")
		return 0, errInstanceIDSizeWrong
	}

	minInstanceID := binary.LittleEndian.Uint64(value)

	lPLG1Imp(d.groupIdx, "ok, min chosen instanceid %d", minInstanceID)

	return minInstanceID, nil
}

func (d *db) setMinChosenInstanceID(wo writeOptions, minInstanceID uint64) error {
	if !d.hasInit {
		lPLG1Err(d.groupIdx, "no init yet")
		return errDBNotInit
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, minInstanceID)

	if err := d.putToLevelDB(true, setMinKey, &string(buf)); err != nil {
		return err
	}

	lPLG1Imp(d.groupIdx, "ok, min chosen instanceid %d", minInstanceID)

	return nil
}

func (d *db) getSystemVariables() ([]byte, error) {
	return d.getFromLevelDB(getSystemVariablesKey)
}

func (d *db) setSystemVariables(wo writeOptions, value []byte) error {
	return d.putToLevelDB(true, setSystemVariablesKey, value)
}

func (d *db) getMasterVariables() ([]byte, error) {
	return d.getFromLevelDB(getMasterVariablesKey)
}

func (d *db) setMasterVariables(wo writeOptions, value []byte) error {
	return d.putToLevelDB(true, setMasterVariablesKey, value)
}

func (d *db) close() {
	if d.valueStore != nil {
		d.valueStore.close()
		d.valueStore = nil
	}

	if d.levelDB != nil {
		d.levelDB.Close()
		d.levelDB = nil
	}
	lPLG1Head(d.groupIdx, "LevelDB Deleted. Path %s", d.dbPath)
}

///////////////////////////////////////////////////////////////////////////////

//multiDatabase implements all the interface of LogStorage
type multiDatabase struct {
	dbList []*db
}

func (m *multiDatabase) init(dbPath string, groupCount int) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		lPLErr("DBPath not exist or no limit to open, %s", dbPath)
		return err
	}

	if groupCount < 1 || groupCount > 100000 {
		lPLErr("Groupcount wrong %d", groupCount)
		return errGroupCountNotProper
	}

	var newDBPath string
	if !strings.HasSuffix(dbPath, "/") {
		newDBPath = dbPath + "/"
	}

	for groupIdx := 0; groupIdx < groupCount; groupIdx++ {
		groupDBPath := fmt.Sprintf("%sg%d", newDBPath, groupIdx)

		d := newDB()
		m.dbList = append(m.dbList, d)

		if err := d.init(groupDBPath, groupIdx); err != nil {
			return err
		}
	}

	lPLImp("OK, DBPath %s groupcount %d", dbPath, groupCount)

	return nil
}

func (m *multiDatabase) GetLogStorageDirPath(groupIdx int) string {
	if groupIdx >= len(m.dbList) {
		return ""
	}

	return m.dbList[groupIdx].getDBPath()
}

func (m *multiDatabase) Get(groupIdx int, instanceID uint64) ([]byte, error) {
	if groupIdx >= len(m.dbList) {
		return "", ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].get(instanceID)
}

func (m *multiDatabase) Put(wo writeOptions, groupIdx int, instanceID uint64, value []byte) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].put(wo, instanceID, value)
}

func (m *multiDatabase) Del(wo writeOptions, groupIdx int, instanceID uint64) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].del(wo, instanceID)
}

func (m *multiDatabase) forceDel(wo writeOptions, groupIdx int, instanceID uint64) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].forceDel(wo, instanceID)
}

func (m *multiDatabase) GetMaxInstanceID(groupIdx int) (uint64, error) {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].getMaxInstanceID()
}

func (m *multiDatabase) SetMinChosenInstanceID(wo writeOptions, groupIdx int, minInstanceID uint64) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].setMinChosenInstanceID(wo, minInstanceID)
}

func (m *multiDatabase) GetMinChosenInstanceID(groupIdx int) (uint64, error) {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].getMinChosenInstanceID()
}

func (m *multiDatabase) ClearAllLog(groupIdx int) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].clearAllLog()
}

func (m *multiDatabase) SetSystemVariables(wo writeOptions, groupIdx int, value []byte) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].setSystemVariables(wo, value)
}

func (m *multiDatabase) GetSystemVariables(groupIdx int) ([]byte, error) {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].getSystemVariables()
}

func (m *multiDatabase) SetMasterVariables(wo writeOptions, groupIdx int, value []byte) error {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].setMasterVariables(wo, value)
}

func (m *multiDatabase) GetMasterVariables(groupIdx int) ([]byte, error) {
	if groupIdx >= len(m.dbList) {
		return ErrGroupIdxOutOfRange
	}

	return m.dbList[groupIdx].getMasterVariables()
}

func (m *multiDatabase) close() {
	if m != nil {
		for _, d := range m.dbList {
			d.close()
		}
	}
}
