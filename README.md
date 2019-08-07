[![Build Status](https://travis-ci.com/AccelByte/messagebus-go-sdk.svg?branch=master)](https://travis-ci.com/AccelByte/messagebus-go-sdk)

# messagebus-go-sdk
Go SDK to connecting AccelByte's justice Go services over message broker

## Supported Message Broker
- Apache Kafka

## Usage

### Importing Package

```go
import "github.com/AccelByte/messagebus-go-sdk"
```

### Publish Asynchronous Message
Publish message asynchronously

```go
client.PublishAsync(
		NewPublish().
			Topic("topic").
			MessageType("message type").
			Message("test message").
			Service("service name").
			TraceID("trace id").
			MessageID("message id"))
```

### Publish Synchronous Message
Publish message synchronously with error response

```go
err := client.PublishSync(
		NewPublish().
			Topic("topic").
			MessageType("message type").
			Message("test message").
			Service("service name").
			TraceID("trace id").
			MessageID("message id"))
```

### Publish Message With Response
Publish message and wait a response from consumer (restful api concepts) with timeout.

```go
client.PublishWithResponses(
		NewPublish().
			Topic("topic").
			MessageType("message type").
			Message("test message").
			TraceID("trace id").
			MessageID("message id").
			Callback(func(message *Message, err error))
```

Set global timeout for publish message with response (in millisecond) 
```go
client.SetTimeout(10000)
```

### Register Callback for Subscribe Message
Register callback function for specific topic and message type.  

```go
client.Register(
		NewSubscribe().
		    Topic("topic").
		    MessageType("message type").
		    Callback(func(message *Message, err error)))
```
