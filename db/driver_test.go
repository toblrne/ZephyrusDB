package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
