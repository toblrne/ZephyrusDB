# ZephyrusDB

concurrent (multithreaded) key value store in Go

## Features:
- concurrency control: mutex locks for thread-safe operations
- LRU cache eviction policy for fast reads 
- WAL (in progress)

## Todo:
- data compaction
- goroutines for async execution + higher throughput 