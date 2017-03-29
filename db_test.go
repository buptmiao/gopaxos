package gopaxos

import (
	"testing"
	"fmt"
)

func TestDBBaseFunctions(t *testing.T) {
	getLoggerInstance().InitLogger(LogLevel_Verbose)
	db := newDB()
	dbpath := "/tmp/gopaxos_test"
	groupIdx := 0
	if err := db.init(dbpath, groupIdx); err != nil {
		panic(err)
	}
	sFileID, err := db.valueToFileID(writeOptions(true), 1234, []byte{1,2,3})
	if err != nil {
		panic(err)
	}
	bytes, instanceID, err := db.fileIDToValue(sFileID)
	fmt.Println(bytes, instanceID, err)
}
