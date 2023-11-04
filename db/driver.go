package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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
}

// New creates a new Driver instance
func New(dir string, logger Logger) (*Driver, error) {
	dir = filepath.Clean(dir)

	if logger == nil {
		logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := &Driver{
		dir: dir,
		log: logger,
	}

	// Create directory if it does not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		logger.Info("Creating the database at '%s' ...\n", dir)
		return driver, os.MkdirAll(dir, 0755)
	}

	logger.Info("Using '%s' (database already exists)\n", dir)
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

	filePath := filepath.Join(d.dir, key)
	value, err := os.ReadFile(filePath)
	if err != nil {
		d.log.Error("Failed to read file: %v", err)
		return nil, err
	}

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
