package ffi

import (
	"testing"
)

func TestCreateIndex(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type User { name: String, email: String }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Create index
	fields := []IndexField{{Name: "email"}}
	index, err := node.CreateIndex("User", "idx_email", fields, true)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if index.Name != "idx_email" {
		t.Errorf("Expected index name 'idx_email', got '%s'", index.Name)
	}
	if !index.Unique {
		t.Error("Expected index to be unique")
	}
	if len(index.Fields) != 1 || index.Fields[0].Name != "email" {
		t.Errorf("Expected field 'email', got %v", index.Fields)
	}
	if index.ID == 0 {
		t.Error("Expected non-zero index ID")
	}
}

func TestGetIndexes(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Product { name: String, price: Int }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Create multiple indexes
	_, err = node.CreateIndex("Product", "idx_name", []IndexField{{Name: "name"}}, false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	_, err = node.CreateIndex("Product", "idx_price", []IndexField{{Name: "price", Descending: true}}, false)
	if err != nil {
		t.Fatalf("Failed to create second index: %v", err)
	}

	// Get indexes
	indexes, err := node.GetIndexes("Product")
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != 2 {
		t.Fatalf("Expected 2 indexes, got %d", len(indexes))
	}

	// Check index names
	names := make(map[string]bool)
	for _, idx := range indexes {
		names[idx.Name] = true
	}
	if !names["idx_name"] || !names["idx_price"] {
		t.Errorf("Expected indexes idx_name and idx_price, got %v", names)
	}
}

func TestDropIndex(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Post { title: String }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Create index
	_, err = node.CreateIndex("Post", "idx_title", []IndexField{{Name: "title"}}, false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes, err := node.GetIndexes("Post")
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}

	// Drop index
	err = node.DropIndex("Post", "idx_title")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Verify index is gone
	indexes, err = node.GetIndexes("Post")
	if err != nil {
		t.Fatalf("Failed to get indexes after drop: %v", err)
	}
	if len(indexes) != 0 {
		t.Errorf("Expected 0 indexes after drop, got %d", len(indexes))
	}
}

func TestDropNonexistentIndex(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Comment { body: String }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Drop non-existent index should error
	err = node.DropIndex("Comment", "nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent index")
	}
}

func TestGetAllIndexes(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add multiple schemas
	_, err = node.AddSchema("type Author { name: String }")
	if err != nil {
		t.Fatalf("Failed to add Author schema: %v", err)
	}

	_, err = node.AddSchema("type Book { title: String }")
	if err != nil {
		t.Fatalf("Failed to add Book schema: %v", err)
	}

	// Create indexes on both
	_, err = node.CreateIndex("Author", "idx_author_name", []IndexField{{Name: "name"}}, false)
	if err != nil {
		t.Fatalf("Failed to create author index: %v", err)
	}

	_, err = node.CreateIndex("Book", "idx_book_title", []IndexField{{Name: "title"}}, true)
	if err != nil {
		t.Fatalf("Failed to create book index: %v", err)
	}

	// Get all indexes
	allIndexes, err := node.GetAllIndexes()
	if err != nil {
		t.Fatalf("Failed to get all indexes: %v", err)
	}

	// Should have indexes for both collections
	if len(allIndexes) != 2 {
		t.Fatalf("Expected 2 collections with indexes, got %d", len(allIndexes))
	}

	// Check Author indexes
	authorIndexes, ok := allIndexes["Author"]
	if !ok {
		t.Error("Expected Author collection in all indexes")
	} else if len(authorIndexes) != 1 || authorIndexes[0].Name != "idx_author_name" {
		t.Errorf("Expected idx_author_name, got %v", authorIndexes)
	}

	// Check Book indexes
	bookIndexes, ok := allIndexes["Book"]
	if !ok {
		t.Error("Expected Book collection in all indexes")
	} else if len(bookIndexes) != 1 || bookIndexes[0].Name != "idx_book_title" {
		t.Errorf("Expected idx_book_title, got %v", bookIndexes)
	}
}

func TestCreateCompositeIndex(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Order { customer: String, date: String, total: Int }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Create composite index
	fields := []IndexField{
		{Name: "customer", Descending: false},
		{Name: "date", Descending: true},
	}
	index, err := node.CreateIndex("Order", "idx_customer_date", fields, false)
	if err != nil {
		t.Fatalf("Failed to create composite index: %v", err)
	}

	if len(index.Fields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(index.Fields))
	}
	if index.Fields[0].Name != "customer" || index.Fields[0].Descending {
		t.Errorf("First field incorrect: %v", index.Fields[0])
	}
	if index.Fields[1].Name != "date" || !index.Fields[1].Descending {
		t.Errorf("Second field incorrect: %v", index.Fields[1])
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	Init()

	node, err := NewNode(NodeOptions{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Add schema
	_, err = node.AddSchema("type Article { title: String }")
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Create index
	_, err = node.CreateIndex("Article", "idx_title", []IndexField{{Name: "title"}}, false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Try to create duplicate
	_, err = node.CreateIndex("Article", "idx_title", []IndexField{{Name: "title"}}, false)
	if err == nil {
		t.Error("Expected error when creating duplicate index")
	}
}
