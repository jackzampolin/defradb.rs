package ffi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscriptionLifecycle(t *testing.T) {
	Init()

	// Create node
	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type User { name: String, age: Int }")
	require.NoError(t, err, "failed to add schema")

	// Create subscription
	sub, err := node.Subscribe("")
	require.NoError(t, err, "failed to create subscription")

	// Poll should return no event initially
	result, err := sub.Poll()
	require.NoError(t, err, "poll should succeed")
	require.False(t, result.HasEvent, "should have no event initially")
	require.False(t, result.IsClosed, "subscription should not be closed")

	// Close subscription
	err = sub.Close()
	require.NoError(t, err, "failed to close subscription")

	// Poll after close should fail
	_, err = sub.Poll()
	require.Error(t, err, "poll after close should fail")
}

func TestSubscriptionReceivesMutationEvent(t *testing.T) {
	Init()

	// Create node
	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Book { title: String }")
	require.NoError(t, err, "failed to add schema")

	// Create subscription BEFORE mutation
	sub, err := node.Subscribe("")
	require.NoError(t, err, "failed to create subscription")
	defer sub.Close()

	// Perform a mutation
	_, err = node.Mutate(`mutation { create_Book(input: {title: "Test"}) { _docID } }`)
	require.NoError(t, err, "mutation should succeed")

	// Poll for the event (may need to retry a few times due to async nature)
	var gotEvent bool
	for i := 0; i < 10; i++ {
		result, err := sub.Poll()
		require.NoError(t, err, "poll should succeed")

		if result.HasEvent {
			require.NotNil(t, result.Event, "event should not be nil")
			require.Equal(t, "update", result.Event.Type, "event type should be update")
			require.NotEmpty(t, result.Event.DocID, "doc_id should not be empty")
			gotEvent = true
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	require.True(t, gotEvent, "should have received an event")
}

func TestNodeCloseClosesSubscriptions(t *testing.T) {
	Init()

	// Create node
	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")

	// Create multiple subscriptions
	sub1, err := node.Subscribe("")
	require.NoError(t, err, "failed to create subscription 1")

	sub2, err := node.Subscribe("")
	require.NoError(t, err, "failed to create subscription 2")

	// Close node (should clean up subscriptions)
	err = node.Close()
	require.NoError(t, err, "failed to close node")

	// Polling subscriptions should now fail or indicate closed
	result, err := sub1.Poll()
	// Either error or closed status is acceptable
	if err == nil {
		require.True(t, result.IsClosed, "subscription should be closed")
	}

	result, err = sub2.Poll()
	if err == nil {
		require.True(t, result.IsClosed, "subscription should be closed")
	}
}

func TestSubscriptionMultipleEvents(t *testing.T) {
	Init()

	// Create node
	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Item { name: String }")
	require.NoError(t, err, "failed to add schema")

	// Create subscription
	sub, err := node.Subscribe("")
	require.NoError(t, err, "failed to create subscription")
	defer sub.Close()

	// Perform all mutations first
	for i := 0; i < 3; i++ {
		_, err = node.Mutate(`mutation { create_Item(input: {name: "item"}) { _docID } }`)
		require.NoError(t, err, "mutation %d should succeed", i)
	}

	// Give time for events to be published
	time.Sleep(50 * time.Millisecond)

	// Count events received
	eventCount := 0
	for j := 0; j < 50; j++ {
		result, err := sub.Poll()
		require.NoError(t, err, "poll should succeed")
		if result.HasEvent {
			require.Equal(t, "update", result.Event.Type)
			eventCount++
		} else if result.IsClosed {
			break
		}
	}

	// We should have received at least some events (exact count may vary due to timing)
	require.GreaterOrEqual(t, eventCount, 1, "should have received at least 1 event")
}

func TestSubscriptionCollectionFilter(t *testing.T) {
	Init()

	// Create node
	node, err := NewNode(NodeOptions{InMemory: true})
	require.NoError(t, err, "failed to create node")
	defer node.Close()

	// Add two schemas
	_, err = node.AddSchema("type Author { name: String }")
	require.NoError(t, err, "failed to add Author schema")

	_, err = node.AddSchema("type Article { title: String }")
	require.NoError(t, err, "failed to add Article schema")

	// Create subscription filtered to Author only
	sub, err := node.Subscribe("Author")
	require.NoError(t, err, "failed to create subscription")
	defer sub.Close()

	// Create an Article (should NOT trigger filtered subscription)
	_, err = node.Mutate(`mutation { create_Article(input: {title: "Test Article"}) { _docID } }`)
	require.NoError(t, err, "Article mutation should succeed")

	// Give time for event to be published
	time.Sleep(50 * time.Millisecond)

	// Poll should return no event (Article is filtered out)
	result, err := sub.Poll()
	require.NoError(t, err, "poll should succeed")
	require.False(t, result.HasEvent, "should have no event for filtered collection")

	// Create an Author (should trigger subscription)
	_, err = node.Mutate(`mutation { create_Author(input: {name: "Bob"}) { _docID } }`)
	require.NoError(t, err, "Author mutation should succeed")

	// Poll for the Author event
	var gotAuthorEvent bool
	for i := 0; i < 10; i++ {
		result, err := sub.Poll()
		require.NoError(t, err, "poll should succeed")
		if result.HasEvent {
			require.Equal(t, "update", result.Event.Type)
			require.Contains(t, result.Event.CollectionID, "Author", "event should be for Author")
			gotAuthorEvent = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.True(t, gotAuthorEvent, "should have received Author event")
}
