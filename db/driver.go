package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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
}

// New creates a new Driver instance
func New(dir string, logger Logger, cacheSize int) (*Driver, error) {
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
	}

	return driver, nil
}

// Put sets the value for a key
func (d *Driver) Put(key string, value []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if key == "" {
		return fmt.Errorf("key is required")
	}

	filePath := filepath.Join(d.dir, key)

	// Write the value to a temporary file first
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, value, 0644); err != nil {
		d.log.Error("Failed to write to temp file: %v", err)
		return err
	}

	// Rename the temp file to make the write operation atomic
	if err := os.Rename(tempPath, filePath); err != nil {
		d.log.Error("Failed to rename temp file: %v", err)
		return err
	}

	// Update the cache with the new value
	d.cache.Add(key, value)

	d.log.Info("Put key: %s", key)

	return nil
}

// Get retrieves the value for a key
func (d *Driver) Get(key string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	// Check the cache first
	if value, ok := d.cache.Get(key); ok {
		d.log.Info("Get key (cache hit): %s", key)
		return value.([]byte), nil
	} else {
		d.log.Debug("Cache miss for key: %s", key) // Log cache miss here
	}

	// If not in cache, read from disk
	filePath := filepath.Join(d.dir, key)
	value, err := os.ReadFile(filePath)
	if err != nil {
		d.log.Error("Failed to read file: %v", err)
		return nil, err
	}

	// Add the read value to the cache
	d.cache.Add(key, value)

	d.log.Info("Get key: %s", key)
	return value, nil
}

// Delete removes a key from the store
func (d *Driver) Delete(key string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if key == "" {
		return fmt.Errorf("key is required")
	}

	filePath := filepath.Join(d.dir, key)

	if err := os.Remove(filePath); err != nil {
		d.log.Error("Failed to delete key: %v", err)
		return err
	}

	if d.cache.Contains(key) {
		d.cache.Remove(key)
		d.log.Info("Deleted key from cache: %s", key)
	} else {
		d.log.Debug("Attempted to delete non-existing key from cache: %s", key)
	}
	d.log.Info("Deleted key from disk: %s", key)
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
