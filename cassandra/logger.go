package cassandra

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/douban/gobeansproxy/config"
	logrus "github.com/sirupsen/logrus"
	rotateLogger "gopkg.in/natefinch/lumberjack.v2"
)

var (
	log = logrus.New()
	dumpLogger = logrus.New()
)

func setLogLevel(logLevel string) {
	l, err := logrus.ParseLevel(logLevel)
	if err != nil {
		log.Warnf("log level no supported will use info level (passed %s)", logLevel)
	}
	log.SetLevel(l)
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
}

type DualWriteErrorMgr struct {
	EFile string
	ELogger *logrus.Logger
}

func NewDualWErrMgr(ecfg *config.DualWErrCfg, logger *logrus.Logger) (*DualWriteErrorMgr, error) {
	if logger == nil {
		logger = dumpLogger
	}

	setLogLevel(ecfg.LoggerLevel)

	// check if target folder exists
	if stat, err := os.Stat(ecfg.DumpToDir); err != nil || !stat.IsDir() {
		return nil, fmt.Errorf("%s is not a dir or not exists", ecfg.DumpToDir)
	}
	
	// set dump Logger
	logger.SetFormatter(&logrus.JSONFormatter{})
	dumpFile := filepath.Join(ecfg.DumpToDir, ecfg.FName)
	logger.SetOutput(&rotateLogger.Logger{
		Filename: dumpFile,
		MaxSize: ecfg.RotateSize,
		Compress: ecfg.Compress,
		MaxAge: ecfg.MaxAges,
		MaxBackups: ecfg.MaxBackups,
	})

	return &DualWriteErrorMgr{
		EFile: dumpFile,
		ELogger: logger,
	}, nil
}

func (e *DualWriteErrorMgr) HandleErr(key, op string, err error) {
	e.ELogger.WithFields(logrus.Fields{
		"key": key,
		"op": op,
	}).Error(err)
}
