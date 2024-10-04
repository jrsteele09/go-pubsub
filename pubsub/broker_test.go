package pubsub_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jrsteele09/go-pubsub/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_SendMoreMessagesThanRequired(t *testing.T) {
	for i := 0; i < 5000; i++ {
		b := pubsub.NewBroker()
		topic := "test"
		message := "hello"

		require.Error(t, b.Publish("non existent topic", []byte("should not be read")))

		require.NoError(t, b.CreateTopic(topic))

		resultSet1 := make([][]byte, 0)
		resultSet2 := make([][]byte, 0)
		resultSet3 := make([][]byte, 0)
		resultSet4 := make([][]byte, 0)

		subscription1, err := b.Subscribe(topic, 1)
		require.NoError(t, err)
		require.NotNil(t, subscription1)

		subscription2, err := b.Subscribe(topic, 10)
		require.NoError(t, err)
		require.NotNil(t, subscription2)

		subscription3, err := b.Subscribe(topic, 20)
		require.NoError(t, err)
		require.NotNil(t, subscription3)

		subscription4, err := b.Subscribe(topic, 30)
		require.NoError(t, err)
		require.NotNil(t, subscription4)

		var wg sync.WaitGroup

		wg.Add(5)
		go ReadNTimes(t, &wg, subscription1, message, 1, &resultSet1)
		go ReadNTimes(t, &wg, subscription2, message, 10, &resultSet2)
		go ReadNTimes(t, &wg, subscription3, message, 20, &resultSet3)
		go ReadNTimes(t, &wg, subscription4, message, 30, &resultSet4)
		go Producer(b, &wg, 400, topic, message)
		wg.Wait()
		b.Close()

		assert.Equal(t, 1, len(resultSet1))
		assert.Equal(t, 10, len(resultSet2))
		assert.Equal(t, 20, len(resultSet3))
		assert.Equal(t, 30, len(resultSet4))
	}
}

func TestBroker_SendLessMessagesAndCloseBroker(t *testing.T) {
	for i := 0; i < 10; i++ {
		b := pubsub.NewBroker()
		topic := "test"
		message := "hello"

		require.NoError(t, b.CreateTopic(topic))

		require.Error(t, b.Publish("non existend topic", []byte("should not be read")))

		resultSet1 := make([][]byte, 0)
		resultSet2 := make([][]byte, 0)
		resultSet3 := make([][]byte, 0)
		resultSet4 := make([][]byte, 0)

		subscription1, err := b.Subscribe(topic, 1)
		require.NoError(t, err)
		require.NotNil(t, subscription1)

		subscription2, err := b.Subscribe(topic, 10)
		require.NoError(t, err)
		require.NotNil(t, subscription2)

		subscription3, err := b.Subscribe(topic, 20)
		require.NoError(t, err)
		require.NotNil(t, subscription3)

		subscription4, err := b.Subscribe(topic, 30)
		require.NoError(t, err)
		require.NotNil(t, subscription4)

		var wg sync.WaitGroup

		wg.Add(5)
		go ReadNTimes(t, &wg, subscription1, message, 1, &resultSet1)
		go ReadNTimes(t, &wg, subscription2, message, 10, &resultSet2)
		go ReadNTimes(t, &wg, subscription3, message, 20, &resultSet3)
		go ReadNTimes(t, &wg, subscription4, message, 30, &resultSet4)
		go Producer(b, &wg, 10, topic, message)
		time.Sleep(100 * time.Millisecond) // Short delay to ensure that all messages are read
		b.Close()
		wg.Wait()

		assert.Equal(t, 1, len(resultSet1))
		assert.Equal(t, 10, len(resultSet2))
		assert.Equal(t, 10, len(resultSet3))
		assert.Equal(t, 10, len(resultSet4))
	}
}

func TestBroker_PublishingOnAClosedBroker(t *testing.T) {
	b := pubsub.NewBroker()
	topic := "test"

	require.Error(t, b.Publish("non existend topic", []byte("should not be read")))
	b.Subscribe(topic, 10)

	b.Close()
	require.Error(t, b.Publish(topic, []byte("should not be read")))
}

func TestBroker_ExistingTopic(t *testing.T) {
	b := pubsub.NewBroker()
	topic := "test"

	require.NoError(t, b.CreateTopic(topic))
	require.Error(t, b.CreateTopic(topic))

	b.Close()
}

func ReadNTimes(t *testing.T, wg *sync.WaitGroup, subscription *pubsub.Subscription, expectedMessage string, n int, resultSet *[][]byte) {
	defer wg.Done()

outer:
	for i := 0; i < n; i++ {
		data, ok := <-subscription.ReceivedData
		if !ok {
			break outer
		}
		assert.Equal(t, expectedMessage, string(data))
		*resultSet = append(*resultSet, data)
	}

	subscription.Close()
}

func Producer(b *pubsub.Broker, wg *sync.WaitGroup, n int, topic string, message string) {
	defer wg.Done()
	for i := 0; i < n; i++ {
		err := b.Publish(topic, []byte(message))
		if err != nil {
			fmt.Printf("Error publishing message: %s\n", err)
		}
	}
}
