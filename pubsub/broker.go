package pubsub

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrBrokerClosed is returned when an action is attempted on a closed broker.
	ErrBrokerClosed = errors.New("pubsub.broker is closed")
)

// Broker represents a message broker that manages topics and their subscribers.
type Broker struct {
	Topics     map[string]*Topic // A map of topic names to topics.
	topicsLock sync.RWMutex      // Mutex to handle concurrent access to the topics.
}

// NewBroker creates and returns a new instance of Broker.
func NewBroker() *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
	}
}

// Subscribe allows a subscriber to subscribe to a topic with a specified buffer size.
// It returns a Subscription to the topic or an error if the topic does not exist.
func (b *Broker) Subscribe(topic string, bufferSize int) (*Subscription, error) {
	t, ok := b.Topics[topic]
	if !ok {
		return nil, errors.New("[Broker Subscribe] topic not found")
	}

	subscription := t.Subscribe(bufferSize)
	return subscription, nil
}

// CreateTopic creates a new topic in the broker. It returns an error if the topic already exists.
func (b *Broker) CreateTopic(topic string) error {
	b.topicsLock.Lock()
	defer b.topicsLock.Unlock()
	_, ok := b.Topics[topic]
	if ok {
		return fmt.Errorf("[Broker CreateTopic] topic %s already exists", topic)
	}

	b.Topics[topic] = NewTopic(topic)
	return nil
}

// Publish sends a message to a specific topic. It returns an error if the topic does not exist
// or if there is a failure in publishing the message.
func (b *Broker) Publish(topic string, message []byte) error {
	b.topicsLock.RLock()
	defer b.topicsLock.RUnlock()

	t, ok := b.Topics[topic]
	if !ok {
		return fmt.Errorf("[Broker Publish] topic %s not found", topic)
	}

	err := t.Publish(message)
	if err != nil {
		return fmt.Errorf("[Broker Publish] error publishing to topic %s: %w", topic, err)
	}

	return nil
}

// Close gracefully closes all topics managed by the broker, ensuring no more messages
// can be published or subscribed to.
func (b *Broker) Close() {
	for _, t := range b.Topics {
		t.Close()
	}
}
