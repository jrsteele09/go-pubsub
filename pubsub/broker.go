package pubsub

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrBrokerClosed = errors.New("pubsub.broker is closed")
)

type Broker struct {
	Topics     map[string]*Topic
	topicsLock sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
	}
}

func (b *Broker) Subscribe(topic string, bufferSize int) (*Subscription, error) {
	t, ok := b.Topics[topic]
	if !ok {
		return nil, errors.New("[Broker Subscribe] topic not found")
	}

	subscription := t.Subscribe(bufferSize)
	return subscription, nil
}

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

func (b *Broker) Close() {
	for _, t := range b.Topics {
		t.Close()
	}
}
