package gopaxos

import (
	"fmt"
	"runtime"
	"sync"
)

type LogLevel int32

const (
	LogLevel_None LogLevel = iota
	LogLevel_Error
	LogLevel_Warning
	LogLevel_Info
	LogLevel_Verbose
)

type LogFunc func(format string, args ...interface{})

var oneLogger sync.Once
var staticLogger *logger

func getLoggerInstance() *logger {
	oneLogger.Do(func() {
		staticLogger = &logger{}
	})
	return staticLogger
}

type logger struct {
	logFunc  LogFunc
	logLevel LogLevel
}

func (l *logger) InitLogger(level LogLevel) {
	l.logLevel = level
}

func (l *logger) SetLogFunc(logFunc LogFunc) {
	l.logFunc = logFunc
}

func (l *logger) LogError(format string, args ...interface{}) {
	newFormat := "\033[41;37m " + format + " \033[0m"
	if l.logFunc != nil {
		l.logFunc(LogLevel_Error, newFormat, args...)
		return
	}
	if l.logLevel < LogLevel_Error {
		return
	}

	fmt.Printf(newFormat, args...)
}

func (l *logger) LogStatus(format string, args ...interface{}) {
	if l.logFunc != nil {
		l.logFunc(LogLevel_Error, format, args...)
		return
	}
	if l.logLevel < LogLevel_Error {
		return
	}
	fmt.Printf(format, args...)
}

func (l *logger) LogWarning(format string, args ...interface{}) {
	newFormat := "\033[44;37m " + format + " \033[0m"
	if l.logFunc != nil {
		l.logFunc(LogLevel_Warning, newFormat, args...)
		return
	}
	if l.logLevel < LogLevel_Warning {
		return
	}

	fmt.Printf(newFormat, args...)
}

func (l *logger) LogInfo(format string, args ...interface{}) {
	newFormat := "\033[45;37m " + format + " \033[0m"
	if l.logFunc != nil {
		l.logFunc(LogLevel_Info, newFormat, args...)
		return
	}
	if l.logLevel < LogLevel_Info {
		return
	}

	fmt.Printf(newFormat, args...)
}

func (l *logger) LogVerbose(format string, args ...interface{}) {
	newFormat := "\033[45;37m " + format + " \033[0m"
	if l.logFunc != nil {
		l.logFunc(LogLevel_Verbose, newFormat, args...)
		return
	}
	if l.logLevel < LogLevel_Verbose {
		return
	}

	fmt.Printf(newFormat, args...)
}

func formatHeader() string {
	_, file, line, _ := runtime.Caller(3)
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	return fmt.Sprintf("%s:%d", file, line)
}

func lNLDebug(format string, args ...interface{}) {
	getLoggerInstance().LogVerbose("DEBUG: %s "+format, formatHeader(), args...)
}

func lNLErr(format string, args ...interface{}) {
	getLoggerInstance().LogError("ERR: %s "+format, formatHeader(), args...)
}

func lPLErr(format string, args ...interface{}) {
	getLoggerInstance().LogError("ERR: %s "+format, formatHeader(), args...)
}

func lPLImp(format string, args ...interface{}) {
	getLoggerInstance().LogInfo("Showy: %s "+format, formatHeader(), args...)
}

func lPLHead(format string, args ...interface{}) {
	getLoggerInstance().LogWarning("Imp: %s "+format, formatHeader(), args...)
}

func lPLDebug(format string, args ...interface{}) {
	getLoggerInstance().LogVerbose("DEBUG: %s "+format, formatHeader(), args...)
}

func lPLGErr(groupIdx int, format string, args ...interface{}) {
	getLoggerInstance().LogError("ERR(%d): %s "+format, groupIdx, formatHeader(), args...)
}

func lPLGStatus(groupIdx int, format string, args ...interface{}) {
	getLoggerInstance().LogStatus("STATUS(%d): %s "+format, groupIdx, formatHeader(), args...)
}

func lPLGImp(groupIdx int, format string, args ...interface{}) {
	getLoggerInstance().LogInfo("Showy(%d): %s "+format, groupIdx, formatHeader(), args...)
}

func lPLGHead(groupIdx int, format string, args ...interface{}) {
	getLoggerInstance().LogWarning("Imp(%d): %s "+format, groupIdx, formatHeader(), args...)
}

func lPLGDebug(groupIdx int, format string, args ...interface{}) {
	getLoggerInstance().LogVerbose("DEBUG(%d): %s "+format, groupIdx, formatHeader(), args...)
}
