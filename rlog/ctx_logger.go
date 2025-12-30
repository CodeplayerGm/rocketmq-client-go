package rlog

import (
	"context"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"path/filepath"
	"strings"
)

type defaultCtxLogger struct {
	logger *logrus.Logger
}

func (l *defaultCtxLogger) Debug(_ context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultCtxLogger) Info(_ context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultCtxLogger) Warning(_ context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultCtxLogger) Error(_ context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Error(msg)
}

func (l *defaultCtxLogger) Fatal(_ context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultCtxLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

func (l *defaultCtxLogger) OutputPath(path string) (err error) {
	config := defaultConfig()
	config.OutputPath = path

	l.logger.Out = config.Logger()
	return
}

type Config struct {
	OutputPath    string
	MaxFileSizeMB int
	MaxBackups    int
	MaxAges       int
	Compress      bool
	LocalTime     bool
}

func (c *Config) Logger() *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   filepath.ToSlash(c.OutputPath),
		MaxSize:    c.MaxFileSizeMB, // MB
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAges,  // days
		Compress:   c.Compress, // disabled by default
		LocalTime:  c.LocalTime,
	}
}

const defaultLogPath = "/tmp/rocketmq-client.log"

func defaultConfig() Config {
	return Config{
		OutputPath:    defaultLogPath,
		MaxFileSizeMB: 10,
		MaxBackups:    5,
		MaxAges:       3,
		Compress:      false,
		LocalTime:     true,
	}
}
