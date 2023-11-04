package main

import (
	"fmt"

	"github.com/toblrne/ZephyrusDBv2/db" // replace with your module path
)

func main() {
	// Define the directory where the key-value store will persist its data
	databaseDir := "./data"

	// Initialize the key-value store with a console logger at INFO level
	store, err := db.New(databaseDir, nil)
	if err != nil {
		fmt.Printf("Failed to initialize the database: %v\n", err)
		return
	}

	// Demonstrate putting a key-value pair
	key := "sampleKey"
	value, err := db.MarshalJson(map[string]string{"hello": "world"})
	if err != nil {
		fmt.Printf("Failed to marshal JSON: %v\n", err)
		return
	}

	if err := store.Put(key, value); err != nil {
		fmt.Printf("Failed to put key-value: %v\n", err)
		return
	}

	// Demonstrate getting a value for a key
	retrievedValue, err := store.Get(key)
	if err != nil {
		fmt.Printf("Failed to get value: %v\n", err)
		return
	}

	fmt.Printf("Retrieved value: %s\n", retrievedValue)

	// Optionally unmarshal the JSON data back into a map
	var data map[string]string
	if err := db.UnmarshalJson(retrievedValue, &data); err != nil {
		fmt.Printf("Failed to unmarshal JSON: %v\n", err)
		return
	}

	fmt.Printf("Unmarshaled data: %#v\n", data)

	// // Demonstrate deleting a key
	// if err := store.Delete(key); err != nil {
	// 	fmt.Printf("Failed to delete key: %v\n", err)
	// 	return
	// }

	// fmt.Println("Key deleted successfully")
}
