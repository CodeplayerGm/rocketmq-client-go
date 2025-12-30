/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package rlog

import (
	"os"
	"strings"

	"context"
	"github.com/sirupsen/logrus"
)

const (
	LogKeyProducerGroup        = "producerGroup"
	LogKeyConsumerGroup        = "consumerGroup"
	LogKeyTopic                = "topic"
	LogKeyMessageQueue         = "MessageQueue"
	LogKeyAllocateMessageQueue = "AllocateMessageQueue"
	LogKeyUnderlayError        = "underlayError"
	LogKeyBroker               = "broker"
	LogKeyValueChangedFrom     = "changedFrom"
	LogKeyValueChangedTo       = "changeTo"
	LogKeyPullRequest          = "PullRequest"
	LogKeyTimeStamp            = "timestamp"
	LogKeyMessageId            = "msgId"
	LogKeyStoreHost            = "storeHost"
	LogKeyQueueId              = "queueId"
	LogKeyQueueOffset          = "queueOffset"
	LogKeyMessages             = "messages"
	LogKeyStack                = "stack"
)

type CtxLogger interface {
	Debug(ctx context.Context, msg string, fields map[string]interface{})
	Info(ctx context.Context, msg string, fields map[string]interface{})
	Warning(ctx context.Context, msg string, fields map[string]interface{})
	Error(ctx context.Context, msg string, fields map[string]interface{})
	Fatal(ctx context.Context, msg string, fields map[string]interface{})
	Level(level string)
	OutputPath(path string) (err error)
}

func init() {
	r := &defaultCtxLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("ROCKETMQ_GO_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		r.logger.SetLevel(logrus.FatalLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	rLog = r
}

var rLog CtxLogger

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger CtxLogger) {
	rLog = logger
}

func SetLogLevel(level string) {
	if level == "" {
		return
	}
	rLog.Level(level)
}

func SetOutputPath(path string) (err error) {
	if "" == path {
		return
	}

	return rLog.OutputPath(path)
}

func Debug(ctx context.Context, msg string, fields map[string]interface{}) {
	rLog.Debug(ctx, msg, fields)
}

func Info(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Info(ctx, msg, fields)
}

func Warning(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Warning(ctx, msg, fields)
}

func Error(ctx context.Context, msg string, fields map[string]interface{}) {
	rLog.Error(ctx, msg, fields)
}

func Fatal(ctx context.Context, msg string, fields map[string]interface{}) {
	rLog.Fatal(ctx, msg, fields)
}
