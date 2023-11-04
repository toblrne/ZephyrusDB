package main

import (
	"fmt"

	"github.com/toblrne/ZephyrusDBv2/api"
	"github.com/toblrne/ZephyrusDBv2/db"
)

func main() {
	// Initialize the db driver
	driver, err := db.New("./data", nil, 25)
	if err != nil {
		fmt.Println("Failed to initialize db:", err)
	}

	// Initialize the API handler
	handler := api.NewHandler(driver)

	// Set up the router
	router := api.InitRouter(handler)

	// Start the server
	router.Run(":8080")
}
