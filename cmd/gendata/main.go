package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/minghe/data-generator/internal/generator"
	"github.com/minghe/data-generator/internal/model"
	"github.com/minghe/data-generator/internal/mongo"
)

func main() {
	var (
		connectionString = flag.String("connection", "", "MongoDB connection string (required)")
		databaseName     = flag.String("database", "testdb", "Database name")
		collectionName   = flag.String("collection", "customers", "Collection name")
		targetSize       = flag.String("size", "1TB", "Target data size (e.g., 1TB, 500GB, 32TB)")
		docSize          = flag.String("doc-size", "auto", "Document size: 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, or auto")
		workers          = flag.Int("workers", 0, "Number of generator workers (0 = auto)")
		writers          = flag.Int("writers", 0, "Number of MongoDB writer workers (0 = auto)")
		batchSize        = flag.Int("batch-size", 0, "Batch size for MongoDB writes (0 = auto)")
		verbose          = flag.Bool("verbose", false, "Verbose logging")
	)
	
	flag.Parse()
	
	if *connectionString == "" {
		log.Fatal("Error: --connection is required")
	}
	
	// Parse target size
	targetBytes, err := parseSize(*targetSize)
	if err != nil {
		log.Fatalf("Error parsing target size: %v", err)
	}
	
	// Determine document size
	docSizeKB, err := determineDocumentSize(*docSize, targetBytes)
	if err != nil {
		log.Fatalf("Error determining document size: %v", err)
	}
	
	if *verbose {
		log.Printf("Target size: %s (%d bytes)", *targetSize, targetBytes)
		log.Printf("Document size: %dKB", docSizeKB/1024)
	}
	
	// Auto-tune workers and batch size for performance
	if *workers == 0 {
		*workers = runtime.NumCPU() * 2
	}
	if *writers == 0 {
		*writers = runtime.NumCPU()
	}
	if *batchSize == 0 {
		*batchSize = 2000 // Larger batches for better throughput
	}
	
	if *verbose {
		log.Printf("Workers: %d, Writers: %d, Batch size: %d", *workers, *writers, *batchSize)
	}
	
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutting down...")
		cancel()
	}()
	
	// Create generator service
	genService := generator.NewService(generator.Config{
		DocumentSize: docSizeKB,
		WorkerCount:  *workers,
		BatchSize:   *batchSize,
		TargetBytes: targetBytes,
	})
	
	// Create MongoDB writer
	mongoWriter, err := mongo.NewWriter(mongo.Config{
		ConnectionString: *connectionString,
		DatabaseName:     *databaseName,
		CollectionName:   *collectionName,
		BatchSize:        *batchSize,
		WriterCount:      *writers,
		TargetBytes:      targetBytes,
	})
	if err != nil {
		log.Fatalf("Failed to create MongoDB writer: %v", err)
	}
	defer mongoWriter.Close()
	
	// Start progress reporter
	progressDone := make(chan bool)
	go reportProgress(ctx, genService, mongoWriter, progressDone)
	
	// Start generation in background
	genErrChan := make(chan error, 1)
	go func() {
		genErrChan <- genService.Generate(ctx)
	}()
	
	// Start writing in background
	writeErrChan := make(chan error, 1)
	go func() {
		writeErrChan <- mongoWriter.Write(ctx, genService.Documents())
	}()
	
	// Wait for completion or error
	select {
	case err := <-genErrChan:
		if err != nil && err != context.Canceled {
			log.Fatalf("Generation error: %v", err)
		}
	case err := <-writeErrChan:
		if err != nil && err != context.Canceled {
			log.Fatalf("Write error: %v", err)
		}
	case <-ctx.Done():
		// Shutdown requested
	}
	
	// Wait a bit for progress reporter to finish
	time.Sleep(500 * time.Millisecond)
	close(progressDone)
	
	// Print final stats
	printFinalStats(genService, mongoWriter)
}

// parseSize parses size strings like "1TB", "500GB", etc.
func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	
	var multiplier int64 = 1
	if strings.HasSuffix(sizeStr, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "TB")
	} else if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "GB")
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "MB")
	} else if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1024
		sizeStr = strings.TrimSuffix(sizeStr, "KB")
	} else if strings.HasSuffix(sizeStr, "B") {
		multiplier = 1
		sizeStr = strings.TrimSuffix(sizeStr, "B")
	}
	
	value, err := strconv.ParseFloat(sizeStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}
	
	return int64(value * float64(multiplier)), nil
}

// determineDocumentSize determines the appropriate document size
func determineDocumentSize(docSizeStr string, targetBytes int64) (model.DocumentSize, error) {
	if docSizeStr != "auto" {
		// Parse explicit size
		switch strings.ToUpper(docSizeStr) {
		case "2KB":
			return model.Size2KB, nil
		case "4KB":
			return model.Size4KB, nil
		case "8KB":
			return model.Size8KB, nil
		case "16KB":
			return model.Size16KB, nil
		case "32KB":
			return model.Size32KB, nil
		case "64KB":
			return model.Size64KB, nil
		default:
			return 0, fmt.Errorf("invalid document size: %s", docSizeStr)
		}
	}
	
	// Auto-select based on target size
	// For very large targets, use larger documents for better throughput
	if targetBytes >= 32*1024*1024*1024*1024 { // 32TB
		return model.Size64KB, nil
	} else if targetBytes >= 16*1024*1024*1024*1024 { // 16TB
		return model.Size32KB, nil
	} else if targetBytes >= 8*1024*1024*1024*1024 { // 8TB
		return model.Size16KB, nil
	} else if targetBytes >= 4*1024*1024*1024*1024 { // 4TB
		return model.Size8KB, nil
	} else if targetBytes >= 2*1024*1024*1024*1024 { // 2TB
		return model.Size4KB, nil
	} else {
		return model.Size2KB, nil
	}
}

// reportProgress periodically reports progress
func reportProgress(ctx context.Context, genService *generator.Service, mongoWriter *mongo.Writer, done chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			genStats := genService.GetStats()
			writeStats := mongoWriter.GetStats()
			
			genMBps := genStats.BytesPerSecond / (1024 * 1024)
			writeMBps := writeStats.BytesPerSecond / (1024 * 1024)
			
			fmt.Printf("\r[Gen: %d docs, %.2f MB/s] [Write: %d docs, %.2f MB/s] [Total: %.2f GB]",
				genStats.DocumentsGenerated,
				genMBps,
				writeStats.DocumentsWritten,
				writeMBps,
				float64(writeStats.BytesWritten)/(1024*1024*1024),
			)
			os.Stdout.Sync()
		}
	}
}

// printFinalStats prints final statistics
func printFinalStats(genService *generator.Service, mongoWriter *mongo.Writer) {
	genStats := genService.GetStats()
	writeStats := mongoWriter.GetStats()
	
	elapsed := writeStats.LastUpdate.Sub(writeStats.StartTime)
	
	fmt.Printf("\n\n=== Final Statistics ===\n")
	fmt.Printf("Total time: %v\n", elapsed.Round(time.Second))
	fmt.Printf("Documents generated: %d\n", genStats.DocumentsGenerated)
	fmt.Printf("Documents written: %d\n", writeStats.DocumentsWritten)
	fmt.Printf("Bytes written: %.2f GB\n", float64(writeStats.BytesWritten)/(1024*1024*1024))
	fmt.Printf("Average generation rate: %.2f docs/sec, %.2f MB/s\n",
		genStats.DocumentsPerSecond,
		genStats.BytesPerSecond/(1024*1024),
	)
	fmt.Printf("Average write rate: %.2f docs/sec, %.2f MB/s\n",
		writeStats.DocumentsPerSecond,
		writeStats.BytesPerSecond/(1024*1024),
	)
	fmt.Printf("Throughput: %.2f GB/min\n", float64(writeStats.BytesWritten)/(1024*1024*1024)/elapsed.Minutes())
}

