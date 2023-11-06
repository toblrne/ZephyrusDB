package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/toblrne/ZephyrusDBv2/api"
	"github.com/toblrne/ZephyrusDBv2/db"
)

func main() {
	// Initialize the db driver
	driver, err := db.New("./data", nil, 25, 16)
	if err != nil {
		fmt.Println("Failed to initialize db:", err)
		return
	}

	// Deserialize the B-tree from the file
	btreeFilePath := "./data/btree.json"
	if err := driver.DeserializeBTree(btreeFilePath); err != nil {
		fmt.Println("Failed to deserialize the B-tree:", err)
		// Handle deserialization failure if necessary
	}

	var wg sync.WaitGroup

	// Setup channel to listen for signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Initialize the API handler
	handler := api.NewHandler(driver)

	// Set up the router
	router := api.InitRouter(handler)

	// Start the server
	fmt.Println("Server starting on :8080")
	wg.Add(1)
	go func() {
		if err := router.Run(":8080"); err != nil {
			fmt.Printf("Server failed to start: %v\n", err)
		}
		wg.Done()
	}()

	// This goroutine executes a graceful shutdown
	go func() {
		<-sigs
		fmt.Println("\nReceived shutdown signal")

		// Serialize the B-tree to the file before exiting
		if err := driver.SerializeBTree(btreeFilePath); err != nil {
			fmt.Println("Failed to serialize the B-tree:", err)
		} else {
			fmt.Println("B-tree successfully serialized to file")
		}

		// Perform any other necessary cleanup

		// Wait for all operations to complete
		wg.Wait()

		os.Exit(0)
	}()

	// Block main goroutine until it's signaled to shut down
	wg.Wait()
}
