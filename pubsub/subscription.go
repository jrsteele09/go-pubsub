package pubsub

// Subscription represents a subscriber to a topic. It contains channels for receiving
// messages and handling subscription completion.
type Subscription struct {
	ID               uint64        // Unique ID for the subscription.
	publishChannel   chan []byte   // Channel where messages are published.
	ReceivedData     chan []byte   // Channel from which the subscriber receives messages.
	SubscriptionDone chan struct{} // Channel to signal that the subscription is closed.
}

// NewSubscription creates and returns a new Subscription instance. It takes a unique ID,
// a channel to signal when the topic is done, and a buffer size for message queues.
func NewSubscription(id uint64, topicDone chan struct{}, bufferSize int) *Subscription {
	s := &Subscription{
		ID:               id,
		SubscriptionDone: make(chan struct{}),
		ReceivedData:     make(chan []byte, bufferSize),
		publishChannel:   make(chan []byte, bufferSize),
	}
	s.start(topicDone)
	return s
}

// start begins the message delivery process for the subscription. It listens for
// published messages, topic completion, and subscription closure.
func (s *Subscription) start(topicDone chan struct{}) {
	go func() {
		for {
			select {
			case data, ok := <-s.publishChannel:
				// If the publish channel is closed, stop the goroutine.
				if !ok {
					return
				}
				s.ReceivedData <- data
			case <-topicDone:
				// When the topic is done, close the ReceivedData channel.
				close(s.ReceivedData)
				return
			case <-s.SubscriptionDone:
				// When the subscription is closed, close the ReceivedData channel.
				close(s.ReceivedData)
				return
			case data := <-s.publishChannel:
				// Deliver the published message to the ReceivedData channel.
				s.ReceivedData <- data
			}
		}
	}()
}

// Close signals the subscription to terminate, stopping the reception of further messages.
func (s *Subscription) Close() {
	close(s.SubscriptionDone)
}
