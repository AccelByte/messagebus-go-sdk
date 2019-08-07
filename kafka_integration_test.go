// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package messagebus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	timeoutTest = 10000
	realm       = "realm"
)

func createKafkaClient(t *testing.T) *KafkaClient {
	t.Helper()

	brokerList := []string{"localhost:9092"}
	client, _ := NewKafkaClient(brokerList, realm)
	return client
}

func cleanupTest(t *testing.T, client *KafkaClient) {
	t.Helper()
	_ = client.asyncProducer.Close()
	_ = client.syncProducer.Close()
	_ = client.consumer.Close()
	_ = client.broker.Close()
}

func timeout(t *testing.T, timeout int) {
	t.Helper()
	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	go func() {
	loop:
		for {
			select {
			case <-timer.C:
				assert.FailNow(t, "register process timeout")
				break loop
			}
		}
	}()
}

func TestKafkaClientRegisterSuccess(t *testing.T) {
	timeout(t, timeoutTest)

	client := createKafkaClient(t)

	defer cleanupTest(t, client)

	message := `{
		"userId": "user123"
	}`

	mockMessage := struct {
		topic       string
		message     []byte
		messageType string
		service     string
		traceId     string
		messageId   string
	}{
		topic:       "testTopic",
		messageType: "testTopic:request",
		messageId:   "message123",
		traceId:     "trace123",
		service:     "service",
		message:     []byte(message),
	}

	doneChan := make(chan bool, 1)

	client.Register(
		NewSubscribe().
			Topic(mockMessage.topic).
			MessageType(mockMessage.messageType).
			Callback(func(message *Message, err error) {
				assert.Equal(t, realm+separator+mockMessage.topic, message.Topic, "topic should be equal")
				assert.Equal(t, mockMessage.messageType, message.MessageType, "message type should be equal")
				assert.Equal(t, mockMessage.messageId, message.MessageId, "message ID should be equal")
				assert.Equal(t, mockMessage.message, message.Message, "message should be equal")
				assert.Equal(t, mockMessage.service, message.Service, "service should be equal")
				assert.Equal(t, mockMessage.traceId, message.TraceId, "trace ID should be equal")
				doneChan <- true
			}))

	client.PublishAsync(
		NewPublish().
			Topic(mockMessage.topic).
			MessageType(mockMessage.messageType).
			Message(mockMessage.message).
			Service(mockMessage.service).
			TraceID(mockMessage.traceId).
			MessageID(mockMessage.messageId))

	<-doneChan
}

func TestKafkaClientPublistWithResponseSuccess(t *testing.T) {
	timeout(t, timeoutTest)

	client := createKafkaClient(t)
	client.SetTimeout(5000)

	defer cleanupTest(t, client)

	message := `{
		"userId": "user123"
	}`

	mockMessage := struct {
		topic       string
		message     []byte
		messageType string
		service     string
		traceId     string
		messageId   string
	}{
		topic:       "testTopic",
		messageType: "testTopic:request",
		messageId:   "message123",
		traceId:     "trace123",
		service:     "service",
		message:     []byte(message),
	}

	doneChan := make(chan bool, 1)

	client.PublishWithResponses(
		NewPublish().
			Topic(mockMessage.topic).
			MessageType(mockMessage.messageType).
			Message(mockMessage.message).
			MessageID(mockMessage.messageId).
			TraceID(mockMessage.traceId).
			Callback(func(message *Message, err error) {
				assert.Equal(t, realm+separator+mockMessage.topic, message.Topic, "topic should be equal")
				assert.Equal(t, mockMessage.messageType, message.MessageType, "message type should be equal")
				assert.Equal(t, mockMessage.messageId, message.MessageId, "message ID should be equal")
				assert.Equal(t, mockMessage.message, message.Message, "message should be equal")
				assert.Equal(t, mockMessage.traceId, message.TraceId, "trace ID should be equal")
				doneChan <- true
			}))

	<-doneChan
}
