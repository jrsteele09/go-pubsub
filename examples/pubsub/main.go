package main

import (
	"fmt"
	"sync"
	"time"

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
		for {
			select {
			case <-subscription.SubscriptionDone:
				return
			case msg, ok := <-subscription.ReceivedData:
				if !ok {
					return
				}
				fmt.Println("Received message:", string(msg))
			}
		}
	}()

	// Publish some messages
	for i := 0; i < 5; i++ {
		broker.Publish(topic, []byte(fmt.Sprintf("Message %d", i+1)))
	}

	// Sleep to enable the subscriber to receive messages
	time.Sleep(10 * time.Millisecond)

	// Close the subscription
	subscription.Close()

	// Wait for the subscriber to complete
	wg.Wait()
	broker.Close()
}
