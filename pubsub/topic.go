package pubsub

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrTopicClosed = errors.New("pubsub.Topic is closed")
)

type Topic struct {
	name           string
	subscribers    map[uint64]*Subscription
	lock           sync.RWMutex
	publishChannel chan []byte
	done           chan struct{}
	subscriberID   atomic.Uint64
}

func NewTopic(name string) *Topic {
	t := &Topic{
		name:        name,
		subscribers: make(map[uint64]*Subscription, 0),
		done:        make(chan struct{}),
	}
	t.publishChannel = t.startPublisher(t.done)
	return t
}

func (t *Topic) startPublisher(done chan struct{}) chan []byte {
	publishChannel := make(chan []byte)
	go func() {
		for {
			select {
			case <-done:
				return

			case data := <-publishChannel:
				t.sendToSubscribers(data)
			}
		}
	}()
	return publishChannel
}

func (t *Topic) sendToSubscribers(data []byte) {
	for _, s := range t.subscribers {
		select {
		case <-t.done:
		case <-s.SubscriptionDone:
		case s.publishChannel <- data:
		}
	}
}

func (t *Topic) Close() {
	close(t.done)
}

func (t *Topic) Subscribe(bufferSize int) *Subscription {
	t.lock.Lock()
	defer t.lock.Unlock()

	s := NewSubscription(t.subscriberID.Add(1), t.done, bufferSize)
	t.subscribers[s.ID] = s
	return s
}

func (t *Topic) Publish(message []byte) error {
	select {
	case <-t.done:
		return ErrTopicClosed
	case t.publishChannel <- message:
	}
	return nil
}
