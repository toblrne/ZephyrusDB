package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/btree"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jcelliott/lumber"
)

type Options struct {
	Logger Logger
}

type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Warn(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
}

type Driver struct {
	mutex sync.RWMutex
	dir   string
	log   Logger
	cache *lru.Cache
	tree  *btree.BTree
}

type item struct {
	Key   string
	Value []byte
}

// Less implements the btree.Item interface for *item
func (i *item) Less(than btree.Item) bool {
	return i.Key < than.(*item).Key
}

// New creates a new Driver instance
func New(dir string, logger Logger, cacheSize int, degree int) (*Driver, error) {
	dir = filepath.Clean(dir)

	// Initialize logger if not provided
	if logger == nil {
		logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	// Create the directory if it does not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		logger.Info("Creating the database at '%s' ...\n", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	} else {
		logger.Info("Using '%s' (database already exists)\n", dir)
	}

	// Initialize the cache with an eviction callback
	cache, err := lru.NewWithEvict(cacheSize, func(key interface{}, value interface{}) {
		logger.Info("Evicted key: %v", key)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %v", err)
	}

	// Create the Driver with the initialized cache
	driver := &Driver{
		dir:   dir,
		log:   logger,
		cache: cache,
		tree:  btree.New(degree),
	}

	return driver, nil
}

func (d *Driver) Put(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Check if the value is different before replacing in the tree or writing to disk
	existingItem, ok := d.tree.Get(&item{Key: key}).(*item)
	if ok && bytes.Equal(existingItem.Value, value) {
		// The key exists and the value is the same, so there's nothing to do.
		return nil
	}

	// Update the cache with the new value (cache Add is thread-safe already so we don't need to lock around it)
	d.cache.Add(key, value)

	// Replace or insert the new item into the B-tree
	d.tree.ReplaceOrInsert(&item{Key: key, Value: value})

	// Write the value to disk, as it has changed or is new
	filePath := filepath.Join(d.dir, key)
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, value, 0644); err != nil {
		d.log.Error("Failed to write to temp file: %v", err)
		return err
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		d.log.Error("Failed to rename temp file: %v", err)
		return err
	}

	d.log.Info("Put key: %s", key)
	return nil
}

// Get retrieves the value for a key
func (d *Driver) Get(key string) ([]byte, error) {

	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	d.mutex.RLock() // Use read lock to allow concurrent reads
	defer d.mutex.RUnlock()

	if value, ok := d.cache.Get(key); ok {
		d.log.Info("Get key (cache hit): %s", key)
		return value.([]byte), nil
	}

	if item, ok := d.tree.Get(&item{Key: key}).(*item); ok {
		d.cache.Add(key, item.Value) // Cache the value
		d.log.Info("Get key (B-tree hit): %s", key)
		return item.Value, nil
	}

	// If not in cache or B-tree, read from disk
	filePath := filepath.Join(d.dir, key)
	value, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			d.log.Debug("Get key not found: %s", key)
			return nil, fmt.Errorf("key not found")
		}
		d.log.Error("Failed to read file: %v", err)
		return nil, err
	}

	// Add the read value to the cache and B-tree
	d.cache.Add(key, value)
	d.tree.ReplaceOrInsert(&item{Key: key, Value: value})
	d.log.Info("Get key: %s", key)

	return value, nil
}

// Delete removes a key from the store
func (d *Driver) Delete(key string) error {

	if key == "" {
		return fmt.Errorf("key is required")
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// First check if the key exists in the B-tree
	if d.tree.Delete(&item{Key: key}) == nil {
		d.log.Debug("Key not found in B-tree: %s", key)
		return fmt.Errorf("key not found")
	}

	// Remove from cache if present
	d.cache.Remove(key)

	// Delete the file
	filePath := filepath.Join(d.dir, key)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) { // Check if the file exists before trying to delete
		d.log.Error("Failed to delete key: %v", err)
		return err
	}

	d.log.Info("Deleted key: %s", key)
	return nil
}

// Marshal an interface into a JSON byte array
func MarshalJson(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal a byte array into an interface
func UnmarshalJson(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// Compact cleans up the directory, removing any temporary or corrupt files
func (d *Driver) Compact() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// List all files in the directory
	files, err := os.ReadDir(d.dir)
	if err != nil {
		d.log.Error("Failed to list directory for compaction: %v", err)
		return err
	}

	// Iterate over all files and perform cleanup
	for _, file := range files {
		filePath := filepath.Join(d.dir, file.Name())

		// Check for temporary files and remove them
		if filepath.Ext(file.Name()) == ".tmp" {
			if err := os.Remove(filePath); err != nil {
				d.log.Error("Failed to remove temporary file during compaction: %v", err)
				continue // Continue with the next file
			}
			d.log.Info("Removed temporary file during compaction: %s", file.Name())
		}
	}

	return nil
}

func (d *Driver) SerializeBTree(filePath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var items []item
	d.tree.Ascend(func(i btree.Item) bool {
		items = append(items, *(i.(*item)))
		return true
	})

	d.log.Info("Items to serialize: %v", items)   // Log the items to be serialized
	d.log.Info("B-tree length: %d", d.tree.Len()) // Log the length of the B-tree

	data, err := json.Marshal(items)
	if err != nil {
		d.log.Error("Error serializing B-tree: %v", err)
		return err
	}

	tempFilePath := filePath + ".tmp"
	if err := os.WriteFile(tempFilePath, data, 0644); err != nil {
		d.log.Error("Error writing serialized data to temp file: %v", err)
		return err
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {
		d.log.Error("Error renaming temp file to final file: %v", err)
		return err
	}

	d.log.Info("Successfully serialized B-tree to %s", filePath)
	return nil
}

func (d *Driver) DeserializeBTree(filePath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		d.log.Error("Error reading serialized B-tree file: %v", err)
		return err
	}

	var items []item
	if err := json.Unmarshal(data, &items); err != nil {
		d.log.Error("Error deserializing B-tree: %v", err)
		return err
	}

	d.log.Info("Items deserialized: %v", items) // Log the items after deserialization

	d.tree.Clear(false)
	for _, itm := range items {
		itmCopy := itm // Create a copy of itm
		d.tree.ReplaceOrInsert(&itmCopy)
	}

	d.log.Info("Successfully deserialized B-tree from %s", filePath)
	d.log.Info("B-tree length after deserialization: %d", d.tree.Len()) // Log the length of the B-tree

	return nil
}
