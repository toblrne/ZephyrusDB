package db

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/btree"
)

func setupDriver(t *testing.T) (*Driver, string) {
	dir, err := os.MkdirTemp("", "btree_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}

	driver, err := New(dir, nil, 128, 2) // assuming cache size of 128 and btree degree of 2 for testing
	if err != nil {
		os.RemoveAll(dir) // Clean up
		t.Fatalf("Failed to create driver: %s", err)
	}

	return driver, dir
}

func TestSerializeAndDeserializeBTree(t *testing.T) {
	driver, dir := setupDriver(t)
	defer os.RemoveAll(dir)

	// Fill the tree with some key-value pairs.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%c", 'A'+i)
		driver.tree.ReplaceOrInsert(&item{Key: key, Value: []byte{byte(i)}})
	}

	// Serialize the tree to a temporary file
	filePath := filepath.Join(dir, "btree.json")
	if err := driver.SerializeBTree(filePath); err != nil {
		t.Errorf("SerializeBTree failed: %s", err)
	}

	// Clear the tree to simulate a fresh start
	driver.tree.Clear(true)

	// Deserialize the tree from the file
	if err := driver.DeserializeBTree(filePath); err != nil {
		t.Errorf("DeserializeBTree failed: %s", err)
	}

	// Verify the tree has the expected number of items
	if got, want := driver.tree.Len(), 10; got != want {
		t.Errorf("tree.Len() = %d, want %d", got, want)
	}

	// Verify the items are as expected
	driver.tree.Ascend(func(i btree.Item) bool {
		it := i.(*item)
		if it.Value[0] != byte(it.Key[0]-'A') {
			t.Errorf("Deserialized item does not match original. Got %v, want %v", it, string('A'+it.Value[0]))
		}
		return true
	})
}

func TestEmptyTreeSerialization(t *testing.T) {
	driver, dir := setupDriver(t)
	defer os.RemoveAll(dir)

	// Attempt to serialize an empty tree
	filePath := filepath.Join(dir, "empty_btree.json")
	if err := driver.SerializeBTree(filePath); err != nil {
		t.Errorf("SerializeBTree failed for an empty tree: %s", err)
	}

	// Clear the tree and deserialize to make sure nothing breaks
	driver.tree.Clear(true)
	if err := driver.DeserializeBTree(filePath); err != nil {
		t.Errorf("DeserializeBTree failed for an empty tree: %s", err)
	}

	if got, want := driver.tree.Len(), 0; got != want {
		t.Errorf("tree.Len() after deserializing an empty tree = %d, want %d", got, want)
	}
}

func TestLargeTreeSerialization(t *testing.T) {
	driver, dir := setupDriver(t)
	defer os.RemoveAll(dir)

	// Fill the tree with a larger number of key-value pairs
	numItems := 1000
	for i := 0; i < numItems; i++ {
		driver.tree.ReplaceOrInsert(&item{Key: fmt.Sprintf("%d", i), Value: []byte{byte(i)}})
	}

	// Serialize and then deserialize
	filePath := filepath.Join(dir, "large_btree.json")
	if err := driver.SerializeBTree(filePath); err != nil {
		t.Errorf("SerializeBTree failed: %s", err)
	}

	driver.tree.Clear(true)
	if err := driver.DeserializeBTree(filePath); err != nil {
		t.Errorf("DeserializeBTree failed: %s", err)
	}

	if got, want := driver.tree.Len(), numItems; got != want {
		t.Errorf("tree.Len() after deserializing a large tree = %d, want %d", got, want)
	}
}

func TestConcurrentSerialization(t *testing.T) {
	driver, dir := setupDriver(t)
	defer os.RemoveAll(dir)

	// Start a goroutine that continuously modifies the tree
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				driver.tree.ReplaceOrInsert(&item{Key: fmt.Sprintf("%d", rand.Int()), Value: []byte{byte(rand.Intn(256))}})
			}
		}
	}()

	// Serialize the tree while it's being modified
	filePath := filepath.Join(dir, "concurrent_btree.json")
	time.Sleep(1 * time.Second) // Wait a bit for the goroutine to start working
	if err := driver.SerializeBTree(filePath); err != nil {
		close(stopCh)
		t.Errorf("SerializeBTree failed during concurrent operations: %s", err)
	}

	close(stopCh)
	// Deserialize should still function correctly
	if err := driver.DeserializeBTree(filePath); err != nil {
		t.Errorf("DeserializeBTree failed after concurrent operations: %s", err)
	}

	// We won't check the tree length here because it's indeterminate
}

func TestTreeIntegrityAfterSerialization(t *testing.T) {
	driver, dir := setupDriver(t)
	defer os.RemoveAll(dir)

	// Create a map to track expected key-value pairs
	expected := make(map[string]byte)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%d", i)
		value := byte(i)
		expected[key] = value
		driver.tree.ReplaceOrInsert(&item{Key: key, Value: []byte{value}})
	}

	// Serialize and deserialize
	filePath := filepath.Join(dir, "integrity_btree.json")
	if err := driver.SerializeBTree(filePath); err != nil {
		t.Errorf("SerializeBTree failed: %s", err)
	}

	driver.tree.Clear(true)
	if err := driver.DeserializeBTree(filePath); err != nil {
		t.Errorf("DeserializeBTree failed: %s", err)
	}

	// Verify the integrity of the tree
	for k, v := range expected {
		searchItem := &item{Key: k}
		found := driver.tree.Get(searchItem).(*item)
		if found == nil || found.Value[0] != v {
			t.Errorf("item with key %s has incorrect value after deserialization. Got %v, want %v", k, found.Value[0], v)
		}
	}
}
