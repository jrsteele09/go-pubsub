package pubsub

type Subscription struct {
	ID               uint64
	publishChannel   chan []byte
	ReceivedData     chan []byte
	SubscriptionDone chan struct{}
}

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

func (s *Subscription) start(topicDone chan struct{}) {
	go func() {
		for {
			select {
			case data, ok := <-s.publishChannel:
				if !ok {
					return
				}
				s.ReceivedData <- data
			case <-topicDone:
				close(s.ReceivedData)
				return
			case <-s.SubscriptionDone:
				close(s.ReceivedData)
				return
			case data := <-s.publishChannel:
				s.ReceivedData <- data
			}
		}
	}()
}

func (s *Subscription) Close() {
	close(s.SubscriptionDone)
}
