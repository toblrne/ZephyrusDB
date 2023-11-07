package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	// Setup channel to listen for signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Initialize the API handler
	handler := api.NewHandler(driver)

	// Set up the router
	router := api.InitRouter(handler)

	// Create the HTTP server
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server failed to start: %v\n", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	<-sigs
	fmt.Println("\nReceived shutdown signal")

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Doesn't block if no connections, but will otherwise wait until the timeout deadline.
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server forced to shutdown: %v\n", err)
	}

	// Serialize the B-tree to the file before exiting
	if err := driver.SerializeBTree(btreeFilePath); err != nil {
		fmt.Println("Failed to serialize the B-tree:", err)
	} else {
		fmt.Println("B-tree successfully serialized to file")
	}

}
