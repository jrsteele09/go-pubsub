package pubsub

// Subscription represents a subscriber to a topic. It contains channels for receiving
// messages and handling subscription completion.
type Subscription struct {
	ID               uint64
	publishChannel   chan []byte
	ReceivedData     chan []byte
	SubscriptionDone chan struct{}
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
			case data, channelOk := <-s.publishChannel:
				if !channelOk { // Closed
					return
				}
				s.ReceivedData <- data
			case <-topicDone: // Topic is done, close the subscription.
				close(s.ReceivedData)
				return
			case <-s.SubscriptionDone: // When the subscription is closed, close the ReceivedData channel.
				close(s.ReceivedData)
				return
			}
		}
	}()
}

// Close signals the subscription to terminate, stopping the reception of further messages.
func (s *Subscription) Close() {
	close(s.SubscriptionDone)
}
