// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package messagebus

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewKafkaClientError(t *testing.T) {
	client, err := NewKafkaClient([]string{"localhost"}, realm)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestKafkaClientPublishSyncSuccess(t *testing.T) {
	config := sarama.NewConfig()
	mockProducer := mocks.NewSyncProducer(t, config)
	mockProducer.ExpectSendMessageAndSucceed()

	defer func() {
		if err := mockProducer.Close(); err != nil {
			t.Error(err)
		}
	}()

	client := KafkaClient{
		syncProducer: mockProducer,
		realm:        realm,
	}

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

	err := client.PublishSync(
		NewPublish().
			Topic(mockMessage.topic).
			MessageType(mockMessage.messageType).
			Message([]byte(mockMessage.message)).
			Service(mockMessage.service).
			TraceID(mockMessage.traceId).
			MessageID(mockMessage.messageId))

	assert.NoError(t, err, "error should be nil")
}

func TestKafkaClientPublishAsyncSuccess(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	mockProducer := mocks.NewAsyncProducer(t, config)
	mockProducer.ExpectInputAndSucceed()

	defer func() {
		if err := mockProducer.Close(); err != nil {
			t.Error(err)
		}
	}()

	client := KafkaClient{
		asyncProducer: mockProducer,
		realm:         realm,
	}

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

	client.PublishAsync(
		NewPublish().
			Topic(mockMessage.topic).
			MessageType(mockMessage.messageType).
			Message([]byte(mockMessage.message)).
			Service(mockMessage.service).
			TraceID(mockMessage.traceId).
			MessageID(mockMessage.messageId))

	actualMessage := <-mockProducer.Successes()

	assert.Equal(t, realm+separator+mockMessage.topic, actualMessage.Topic, "topic should be equal")
}
