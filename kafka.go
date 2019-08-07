// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package messagebus

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"

	"time"
)

var (
	rtoError = errors.New("request time out in publish with response")

	subscribeMap map[string]map[string]func(message *Message, err error)

	publishResponseTimeout = time.Duration(10000)
	topicTimeout           = time.Duration(15000)
)

// KafkaClient wraps client's functionality for Kafka
type KafkaClient struct {
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
	consumer      sarama.Consumer
	broker        *sarama.Broker
	realm         string
}

// PublishBuilderEntry defines data structure to use with Kafka client
type PublishBuilderEntry struct {
	PublishBuilder

	encoded []byte
	err     error
}

func (publishBuilderEntry *PublishBuilderEntry) ensureEncoded() {
	if publishBuilderEntry.encoded == nil && publishBuilderEntry.err == nil {

		if publishBuilderEntry.traceId == "" {
			publishBuilderEntry.traceId = generateID()
		}

		if publishBuilderEntry.messageId == "" {
			publishBuilderEntry.messageId = generateID()
		}

		publishBuilderEntry.encoded, publishBuilderEntry.err = json.Marshal(Message{
			Message:     publishBuilderEntry.message,
			MessageType: publishBuilderEntry.messageType,
			Service:     publishBuilderEntry.service,
			TraceId:     publishBuilderEntry.traceId,
			MessageId:   publishBuilderEntry.messageId,
		})
	}
}

// Encode PublishBuilder into array of bytes
func (publishBuilderEntry *PublishBuilderEntry) Encode() ([]byte, error) {
	publishBuilderEntry.ensureEncoded()
	return publishBuilderEntry.encoded, publishBuilderEntry.err
}

// Length returns size of encoded value
func (publishBuilderEntry *PublishBuilderEntry) Length() int {
	publishBuilderEntry.ensureEncoded()
	return len(publishBuilderEntry.encoded)
}

// NewKafkaClient create a new instance of KafkaClient
func NewKafkaClient(brokerList []string, realm string) (*KafkaClient, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	// currently only support 1 message broker
	broker := sarama.NewBroker(brokerList[0])
	err := broker.Open(config)
	if err != nil {
		logrus.Error("unable to open kafka")
		return nil, err
	}
	if connected, err := broker.Connected(); !connected {
		logrus.Error("unable  connect to kafka")
		return nil, err
	}

	configAsync := sarama.NewConfig()

	asyncProducer, err := sarama.NewAsyncProducer(brokerList, configAsync)
	if err != nil {
		logrus.Error("unable to create async producer in kafka : ", err)
		return nil, err
	}

	configSync := sarama.NewConfig()
	configSync.Producer.Return.Successes = true
	syncProducer, err := sarama.NewSyncProducer(brokerList, configSync)
	if err != nil {
		logrus.Error("unable to create sync producer in kafka : ", err)
		return nil, err
	}

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		logrus.Error("Unable to create consumer in kafka : ", err)
		return nil, err
	}

	client := &KafkaClient{
		asyncProducer,
		syncProducer,
		consumer,
		broker,
		realm,
	}

	subscribeMap = make(map[string]map[string]func(message *Message, err error))

	go listenAsyncError(asyncProducer)
	go cleanup(client)
	return client, nil
}

func cleanup(client *KafkaClient) {
	// define signal notify
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	func() {
		for {
			select {
			case <-sig:
				_ = client.asyncProducer.Close()
				_ = client.syncProducer.Close()
				_ = client.consumer.Close()
				_ = client.broker.Close()
			}
		}
	}()
}

func listenAsyncError(producer sarama.AsyncProducer) {
	for err := range producer.Errors() {
		logrus.Error("unable to publish message using async producer to kafka : ", err)
	}
}

func constructTopic(realm, topic string) string {
	return realm + separator + topic
}

// SetTimeout set listening timeout for publish with response
func (client *KafkaClient) SetTimeout(timeout int) {
	publishResponseTimeout = time.Duration(timeout)
}

// PublishAsync push a message to message broker topic asynchronously
func (client *KafkaClient) PublishAsync(publishBuilder *PublishBuilder) {

	// send message
	client.asyncProducer.Input() <- &sarama.ProducerMessage{
		Timestamp: time.Now().UTC(),
		Value: &PublishBuilderEntry{
			PublishBuilder: *publishBuilder,
		},
		Topic: constructTopic(client.realm, publishBuilder.topic),
	}
}

// PublishSync push a message to message broker topic synchronously
func (client *KafkaClient) PublishSync(publishBuilder *PublishBuilder) error {

	// send message
	_, _, err := client.syncProducer.SendMessage(&sarama.ProducerMessage{
		Timestamp: time.Now().UTC(),
		Value: &PublishBuilderEntry{
			PublishBuilder: *publishBuilder,
		},
		Topic: constructTopic(client.realm, publishBuilder.topic),
	})
	if err != nil {
		logrus.Error("unable to publish message using sync producer to kafka : ", err)
	}

	return err
}

// PublishWithResponses push a message to message broker topic synchronously and waiting response consumer until timeout
// Intended for Point to Point Communication
func (client *KafkaClient) PublishWithResponses(publishBuilder *PublishBuilder) {
	topic := constructTopic(client.realm, publishBuilder.topic)
	// register callback into map with topic and message Type as a key
	// need to create topic, if the keys doesn't exists
	if registerCallback(topic, publishBuilder.messageType, publishBuilder.callback) {
		logrus.Warn("topic and message type already registered")
	} else {
		defer func() {
			err := deleteTopic(client.broker, topic)
			if err != nil {
				logrus.Error("unable to delete topic in kafka : ", err)
				return
			}
		}()

		err := createTopic(client.broker, topic)
		if err != nil {
			logrus.Error("unable to create topic in kafka : ", err)
			return
		}

		// listening a topic
		go func() {
			consumer, err := client.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
			if err != nil {
				logrus.Error("unable to consume topic from kafka : ", err)
				return
			}

			for timeout := time.After(time.Duration(publishResponseTimeout) * time.Millisecond); ; {
				select {
				case consumerMessage := <-consumer.Messages():
					if !processResponseMessage(consumerMessage, publishBuilder.messageId) {
						continue
					}
					break
				case err = <-consumer.Errors():
					callback := publishBuilder.callback
					callback(nil, err)
					break
				case <-timeout:
					callback := publishBuilder.callback
					callback(nil, rtoError)
					break
				}

			}
		}()
	}

	// send message
	_, _, err := client.syncProducer.SendMessage(&sarama.ProducerMessage{
		Timestamp: time.Now().UTC(),
		Value: &PublishBuilderEntry{
			PublishBuilder: *publishBuilder,
		},
		Topic: topic,
	})
	if err != nil {
		logrus.Error("unable to publish message in publish with response to kafka : ", err)
		return
	}
}

// Register add subscriber for a topic and register callback function
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) {
	topic := constructTopic(client.realm, subscribeBuilder.topic)
	if registerCallback(topic, subscribeBuilder.messageType, subscribeBuilder.callback) {
		logrus.Error("topic and message type already registered")
		return
	}

	err := createTopic(client.broker, topic)
	if err != nil {
		logrus.Error("unable to create topic in kafka : ", err)
		return
	}

	// listening a topic
	// each topic would have his own goroutine
	go func() {
		consumer, err := client.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			logrus.Error("unable to consume topic from kafka : ", err)
			return
		}

		for {
			select {
			case consumerMessage := <-consumer.Messages():
				processMessage(consumerMessage)
				break
			case err = <-consumer.Errors():
				callback := subscribeBuilder.callback
				callback(nil, err)
				break
			}

		}
	}()
}

// createTopic create kafka topic
func createTopic(broker *sarama.Broker, topicName string) error {
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(1)
	topicDetail.ReplicationFactor = int16(1)
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topicName] = topicDetail
	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	_, err := broker.CreateTopics(&request)
	return err
}

// deleteTopic delete kafka topic
func deleteTopic(broker *sarama.Broker, topicName string) error {
	request := sarama.DeleteTopicsRequest{
		Timeout: time.Second * topicTimeout,
		Topics:  []string{topicName},
	}

	_, err := broker.DeleteTopics(&request)
	return err
}

// unmarshal unmarshal received message into message struct
func unmarshal(consumerMessage *sarama.ConsumerMessage) *Message {
	var receivedMessage Message
	err := json.Unmarshal(consumerMessage.Value, &receivedMessage)
	if err != nil {
		logrus.Error("unable to unmarshal message from consumer in kafka : ", err)
		return &Message{}
	}
	return &receivedMessage
}

// registerCallback add callback to map with topic and message Type as a key
func registerCallback(topic, messageType string, callback func(message *Message, err error)) (isRegistered bool) {
	if subscribeMap == nil {
		subscribeMap = make(map[string]map[string]func(message *Message, err error))
	}

	if callbackMap, isTopic := subscribeMap[topic]; isTopic {
		if _, isMsgType := callbackMap[messageType]; isMsgType {
			return true
		}
	}

	newCallbackMap := make(map[string]func(message *Message, err error))
	newCallbackMap[messageType] = callback
	subscribeMap[topic] = newCallbackMap
	return false
}

// runCallback run callback function when receive a message
func runCallback(receivedMessage *Message, consumerMessage *sarama.ConsumerMessage) {
	callback := subscribeMap[consumerMessage.Topic][receivedMessage.MessageType]

	if callback == nil {
		logrus.Error(fmt.Sprintf("callback not found for topic : %s, message type : %s", consumerMessage.Topic,
			receivedMessage.MessageType))
		return
	}

	go callback(&Message{
		Topic:       consumerMessage.Topic,
		Message:     receivedMessage.Message,
		MessageType: receivedMessage.MessageType,
		Service:     receivedMessage.Service,
		TraceId:     receivedMessage.TraceId,
		MessageId:   receivedMessage.MessageId,
	}, nil)
}

// processMessage process a message from kafka
func processMessage(consumerMessage *sarama.ConsumerMessage) {
	receivedMessage := unmarshal(consumerMessage)
	runCallback(receivedMessage, consumerMessage)
}

// processResponseMessage process a message from kafka for publish with responses function
func processResponseMessage(consumerMessage *sarama.ConsumerMessage, messageID string) bool {
	receivedMessage := unmarshal(consumerMessage)

	// check the request and response message ID should be equal
	if messageID != receivedMessage.MessageId {
		return false
	}

	runCallback(receivedMessage, consumerMessage)
	return true
}
