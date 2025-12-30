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

package consumer

import (
	"context"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/rlog"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func mockB4Start(c *pushConsumer) {
	c.topicSubscribeInfoTable.Store("TopicTest", []*primitive.MessageQueue{})
}

func TestStart(t *testing.T) {
	Convey("test Start method", t, func() {
		c, _ := NewPushConsumer(
			context.Background(),
			WithGroupName("testGroup"),
			WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
			WithConsumerModel(BroadCasting),
		)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		client := internal.NewMockRMQClient(ctrl)
		c.client = client

		err := c.Subscribe(context.Background(), "TopicTest", MessageSelector{}, func(ctx context.Context,
			msgs ...*primitive.MessageExt) (ConsumeResult, error) {
			rlog.Info(ctx, "Subscribe Callback", map[string]interface{}{
				"msgs": msgs,
			})
			return ConsumeSuccess, nil
		})

		_, exists := c.subscriptionDataTable.Load("TopicTest")
		So(exists, ShouldBeTrue)

		err = c.Unsubscribe(context.Background(), "TopicTest")
		So(err, ShouldBeNil)
		_, exists = c.subscriptionDataTable.Load("TopicTest")
		So(exists, ShouldBeFalse)

		err = c.Subscribe(context.Background(), "TopicTest", MessageSelector{}, func(ctx context.Context,
			msgs ...*primitive.MessageExt) (ConsumeResult, error) {
			rlog.Info(ctx, "Subscribe Callback", map[string]interface{}{
				"msgs": msgs,
			})
			return ConsumeSuccess, nil
		})

		_, exists = c.subscriptionDataTable.Load("TopicTest")
		So(exists, ShouldBeTrue)

		client.EXPECT().ClientID().Return("127.0.0.1@DEFAULT")
		client.EXPECT().Start(gomock.Any()).Return()
		client.EXPECT().RegisterConsumer(gomock.Any(), gomock.Any()).Return(nil)
		client.EXPECT().UpdateTopicRouteInfo(gomock.Any()).AnyTimes().Return()

		Convey("test topic route info not found", func() {
			client.EXPECT().Shutdown(gomock.Any()).Return()
			client.EXPECT().UnregisterConsumer(gomock.Any()).Return()
			err = c.Start(context.Background())
			So(err.Error(), ShouldContainSubstring, "route info not found")
		})

		Convey("test topic route info found", func() {
			client.EXPECT().RebalanceImmediately(gomock.Any()).Return()
			client.EXPECT().CheckClientInBroker().Return()
			client.EXPECT().SendHeartbeatToAllBrokerWithLock(gomock.Any()).Return()
			mockB4Start(c)
			err = c.Start(context.Background())
			So(err, ShouldBeNil)
		})
	})
}
