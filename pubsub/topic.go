package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrTopicClosed is returned when an action is attempted on a closed topic.
	ErrTopicClosed = errors.New("pubsub.Topic is closed")
)

// Topic represents a pub/sub topic where messages can be published and subscribers can
// subscribe to receive those messages.
type Topic struct {
	name           string                   // The name of the topic.
	subscribers    map[uint64]*Subscription // A map of subscribers, indexed by their unique ID.
	lock           sync.RWMutex             // Mutex to handle concurrent access to subscribers.
	publishChannel chan []byte              // Channel where messages are published to subscribers.
	done           chan struct{}            // Channel to signal that the topic is closed.
	subscriberID   atomic.Uint64            // Atomically incrementing counter for subscriber IDs.
}

// NewTopic creates and returns a new Topic instance with the specified name.
func NewTopic(name string) *Topic {
	t := &Topic{
		name:        name,
		subscribers: make(map[uint64]*Subscription, 0),
		done:        make(chan struct{}),
	}
	t.publishChannel = t.startPublisher(t.done)
	return t
}

// startPublisher starts a goroutine that listens on the publish channel and sends
// messages to all subscribers. It runs until the topic is closed.
func (t *Topic) startPublisher(done chan struct{}) chan []byte {
	publishChannel := make(chan []byte)
	go func() {
		for {
			select {
			case <-done:
				// Stop the publisher when the topic is done.
				return
			case data := <-publishChannel:
				// Send the published message to all active subscribers.
				t.sendToSubscribers(data)
			}
		}
	}()
	return publishChannel
}

// sendToSubscribers sends the given message to all subscribers of the topic.
// It skips subscribers whose subscriptions are closed.
func (t *Topic) sendToSubscribers(data []byte) {
	for _, s := range t.subscribers {
		select {
		case <-t.done:
			// Skip if the topic is closed.
		case <-s.SubscriptionDone:
			// Skip if the subscriber is done.
		case s.publishChannel <- data:
			// Send the data to the subscriber's publish channel.
		}
	}
}

// Close closes the topic, signaling that no further messages will be published.
// All subscribers will stop receiving messages once the topic is closed.
func (t *Topic) Close() {
	close(t.done)
}

// Subscribe adds a new subscriber to the topic with the specified buffer size for
// receiving messages. It returns a Subscription instance for the subscriber.
func (t *Topic) Subscribe(bufferSize int) *Subscription {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Create a new subscription and add it to the subscribers map.
	s := NewSubscription(t.subscriberID.Add(1), t.done, bufferSize)
	t.subscribers[s.ID] = s
	return s
}

// Publish publishes a message to the topic. It returns an error if the topic is closed.
func (t *Topic) Publish(message []byte) error {
	select {
	case <-t.done:
		// Return an error if the topic is closed.
		return ErrTopicClosed
	case t.publishChannel <- message:
		// Publish the message to the topic's channel.
	}
	return nil
}
