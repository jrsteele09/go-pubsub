# go-pubsub

[![Go Report Card](https://goreportcard.com/badge/github.com/jrsteele09/go-pubsub)](https://goreportcard.com/report/github.com/jrsteele09/go-pubsub)
[![GoDoc](https://pkg.go.dev/badge/github.com/jrsteele09/go-pubsub)](https://pkg.go.dev/github.com/jrsteele09/go-pubsub)

`go-pubsub` is a simple publish/subscribe (pub/sub) library written in Go. It provides a lightweight message broker that allows multiple subscribers to subscribe to topics and receive messages in a concurrent and thread-safe manner.

## Features

- Multiple subscribers can subscribe to a topic and receive messages.
- Subscribers can specify the number of messages they expect to receive.
- Supports creating, publishing, and subscribing to multiple topics.
- Gracefully handles closing of the broker and subscriptions.
- Error handling for non-existent topics and closed brokers.

## Installation

To use the `go-pubsub` library, you can simply run:

```bash
go get github.com/jrsteele09/go-pubsub
```

## Usage

Here is a basic example of how to use the go-pubsub package:

### Creating a Broker and a Topic

```
package main

import (
    "fmt"
    "github.com/jrsteele09/go-pubsub/pubsub"
)

func main() {
    // Create a new broker
    broker := pubsub.NewBroker()

    // Create a new topic
    topic := "example-topic"
    err := broker.CreateTopic(topic)
    if err != nil {
        fmt.Println("Error creating topic:", err)
        return
    }

    // Publish a message to the topic
    message := []byte("Hello, Pub/Sub!")
    err = broker.Publish(topic, message)
    if err != nil {
        fmt.Println("Error publishing message:", err)
    }

    broker.Close()
}
```

### Subscribing to a Topic

```
package main

import (
    "fmt"
    "sync"
    "github.com/jrsteele09/go-pubsub/pubsub"
)

func main() {
    broker := pubsub.NewBroker()
    topic := "example-topic"
    broker.CreateTopic(topic)

    // Subscribe to the topic, expecting 5 messages
    subscription, err := broker.Subscribe(topic, 5)
    if err != nil {
        fmt.Println("Error subscribing:", err)
        return
    }

    // Start a goroutine to read messages from the subscription
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        for msg := range subscription.ReceivedData {
            fmt.Println("Received message:", string(msg))
        }
    }()

    // Publish some messages
    for i := 0; i < 5; i++ {
        broker.Publish(topic, []byte(fmt.Sprintf("Message %d", i+1)))
    }

    // Wait for the subscriber to finish processing
    wg.Wait()
    broker.Close()
}

```
