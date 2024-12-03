package logger

import (
	"beats/config"
	"beats/utils"
	"fmt"
	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

var (
	logLevelMap = map[string]logrus.Level{
		"error": logrus.ErrorLevel,
		"warn":  logrus.WarnLevel,
		"info":  logrus.InfoLevel,
		"debug": logrus.DebugLevel,
	}

	maxSize    = 10 // 每个日志文件最大10MB
	maxBackups = 3  // 保留最近的3个日志文件
	maxAge     = 7  // 保留最近7天的日志

	defaultLogger *logrus.Logger
	defaultLevel  = logrus.DebugLevel
	defaultLogDir = "/var/log/gse/"
)

func InitLogger(c config.Logger) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	if c.Level != "" {
		if v, ok := logLevelMap[c.Level]; ok {
			logger.SetLevel(v)
		}
	} else {
		logger.SetLevel(defaultLevel)
	}

	var logWritePath string
	execName := utils.GetExecName()
	if c.Path != "" {
		logWritePath = filepath.Join(c.Path, execName+".log")
	} else {
		logWritePath = filepath.Join(defaultLogDir, execName+".log")
	}

	logDir := filepath.Dir(logWritePath)
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			fmt.Printf("Failed to create log directory: %v\n", err)
			os.Exit(1)
		}
	}
	// 检查文件路径是否可写
	if err := utils.CheckPathWritable(logWritePath); err != nil {
		fmt.Printf("Log path is not writable: %v\n", err)
		os.Exit(1)
	}
	// 设置日志输出到文件，并使用 lumberjack 进行日志切分
	logger.SetOutput(&lumberjack.Logger{
		Filename:   logWritePath,
		MaxSize:    maxSize,    // 单个日志文件的最大尺寸（MB）
		MaxBackups: maxBackups, // 保留的旧日志文件的最大数量
		MaxAge:     maxAge,     // 保留的旧日志文件的最大天数
		Compress:   true,       // 是否压缩旧日志文件
	})
	defaultLogger = logger
}

func Debug(args ...interface{}) {
	defaultLogger.Debug(args...)
}

func Debugln(args ...interface{}) {
	defaultLogger.Debugln(args...)
}

func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

func Info(args ...interface{}) {
	defaultLogger.Info(args...)
}

func Infoln(args ...interface{}) {
	defaultLogger.Infoln(args...)
}

func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

func Warn(args ...interface{}) {
	defaultLogger.Warn(args...)
}

func Warnln(args ...interface{}) {
	defaultLogger.Warnln(args...)
}

func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

func Error(args ...interface{}) {
	defaultLogger.Error(args...)
}

func Errorln(args ...interface{}) {
	defaultLogger.Errorln(args...)
}

func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}
