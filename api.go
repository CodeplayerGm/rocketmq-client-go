/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type Producer interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error),
		msg ...*primitive.Message) error
	SendOneWay(ctx context.Context, mq ...*primitive.Message) error
	Request(ctx context.Context, ttl time.Duration, msg *primitive.Message) (*primitive.Message, error)
	RequestAsync(ctx context.Context, ttl time.Duration, callback internal.RequestCallback, msg *primitive.Message) error
}

func NewProducer(ctx context.Context, opts ...producer.Option) (Producer, error) {
	return producer.NewDefaultProducer(ctx, opts...)
}

type TransactionProducer interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
	SendMessageInTransaction(ctx context.Context, mq *primitive.Message) (*primitive.TransactionSendResult, error)
}

func NewTransactionProducer(ctx context.Context, listener primitive.TransactionListener, opts ...producer.Option) (TransactionProducer, error) {
	return producer.NewTransactionProducer(ctx, listener, opts...)
}

type PushConsumer interface {
	// Start the PushConsumer for consuming message
	Start(ctx context.Context) error

	// Shutdown the PushConsumer, all offset of MessageQueue will be sync to broker before process exit
	Shutdown(ctx context.Context) error
	// Subscribe a topic for consuming
	Subscribe(ctx context.Context, topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error

	// Unsubscribe a topic
	Unsubscribe(ctx context.Context, topic string) error

	// Suspend the consumption
	Suspend(ctx context.Context)

	// Resume the consumption
	Resume(ctx context.Context)

	// GetOffsetDiffMap Get offset difference map
	GetOffsetDiffMap(ctx context.Context) map[string]int64
}

func NewPushConsumer(ctx context.Context, opts ...consumer.Option) (PushConsumer, error) {
	return consumer.NewPushConsumer(ctx, opts...)
}

type PullConsumer interface {
	// Start the PullConsumer for consuming message
	Start(ctx context.Context) error
	// GetTopicRouteInfo get topic route info
	GetTopicRouteInfo(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)

	// Subscribe a topic for consuming
	Subscribe(ctx context.Context, topic string, selector consumer.MessageSelector) error

	// Unsubscribe a topic
	Unsubscribe(ctx context.Context, topic string) error

	// Assign assign message queue to consumer
	Assign(ctx context.Context, topic string, mqs []*primitive.MessageQueue) error

	// Shutdown the PullConsumer, all offset of MessageQueue will be commit to broker before process exit
	Shutdown(ctx context.Context) error

	// Poll messages with timeout.
	Poll(ctx context.Context, timeout time.Duration) (*consumer.ConsumeRequest, error)

	// ACK ACK
	ACK(ctx context.Context, cr *consumer.ConsumeRequest, consumeResult consumer.ConsumeResult)

	// Pull message of topic,  selector indicate which queue to pull.
	Pull(ctx context.Context, numbers int) (*primitive.PullResult, error)

	// PullFrom pull messages of queue from the offset to offset + numbers
	PullFrom(ctx context.Context, queue *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error)

	// SeekOffset seek offset for specific queue
	SeekOffset(ctx context.Context, queue *primitive.MessageQueue, offset int64)

	// OffsetForTimestamp get offset of specific queue with timestamp
	OffsetForTimestamp(ctx context.Context, queue *primitive.MessageQueue, timestamp int64) (int64, error)

	// UpdateOffset updateOffset update offset of queue in mem
	UpdateOffset(ctx context.Context, queue *primitive.MessageQueue, offset int64) error

	// PersistOffset persist all offset in mem.
	PersistOffset(ctx context.Context, topic string) error

	PersistOffsetSync(ctx context.Context) error

	// CurrentOffset return the current offset of queue in mem.
	CurrentOffset(ctx context.Context, queue *primitive.MessageQueue) (int64, error)
}

func NewPullConsumer(ctx context.Context, opts ...consumer.Option) (PullConsumer, error) {
	return consumer.NewPullConsumer(ctx, opts...)
}
