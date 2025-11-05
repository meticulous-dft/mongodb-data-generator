package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/meticulous-dft/mongodb-data-generator/internal/logger"
	"github.com/meticulous-dft/mongodb-data-generator/internal/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"golang.org/x/sync/errgroup"
)

// Writer handles bulk writing to MongoDB
type Writer struct {
	client       *mongo.Client
	collection   *mongo.Collection
	batchSize    int
	writerCount  int
	targetBytes  int64
	bytesWritten int64
	docsWritten  int64
	mu           sync.RWMutex
	startTime    time.Time
	ycsbLogger   *logger.YCSBLogger
}

// Config holds writer configuration
type Config struct {
	ConnectionString string
	DatabaseName     string
	CollectionName   string
	BatchSize        int
	WriterCount      int
	TargetBytes      int64
	YCSBLogger       *logger.YCSBLogger
}

// NewWriter creates a new MongoDB writer
func NewWriter(config Config) (*Writer, error) {
	if config.DatabaseName == "" {
		config.DatabaseName = "testdb"
	}
	if config.CollectionName == "" {
		config.CollectionName = "customers"
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.WriterCount <= 0 {
		config.WriterCount = 5 // Multiple writers for better throughput
	}

	// Append compressors=disabled to connection string to disable compression
	connectionString := config.ConnectionString
	if !strings.Contains(connectionString, "compressors=") {
		separator := "&"
		if !strings.Contains(connectionString, "?") {
			separator = "?"
		}
		connectionString = connectionString + separator + "compressors=disabled"
	}
	
	// Create MongoDB client with optimized settings
	// Use W:1, J:false for maximum throughput
	wc := writeconcern.New(writeconcern.W(1), writeconcern.J(false))
	
	clientOptions := options.Client().
		ApplyURI(connectionString).
		SetMaxPoolSize(uint64(config.WriterCount * 10)).
		SetMinPoolSize(uint64(config.WriterCount)).
		SetWriteConcern(wc).
		SetRetryWrites(false).
		SetServerSelectionTimeout(30 * time.Second).
		SetSocketTimeout(60 * time.Second)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}
	
	database := client.Database(config.DatabaseName)
	
	// Create collection with WiredTiger storage compression disabled
	// This ensures storage size matches logical size for performance testing
	createOpts := options.CreateCollection().
		SetStorageEngine(bson.D{
			{Key: "wiredTiger", Value: bson.D{
				{Key: "configString", Value: "block_compressor=none"},
			}},
		})
	
	// Try to create collection (ignore error if it already exists)
	err = database.CreateCollection(ctx, config.CollectionName, createOpts)
	if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "NamespaceExists") {
		// If collection creation fails for other reasons, log but continue
		// The collection might already exist or we might not have permissions
		// In that case, we'll use the existing collection
	}
	
	collection := database.Collection(config.CollectionName)

	return &Writer{
		client:      client,
		collection:  collection,
		batchSize:   config.BatchSize,
		writerCount: config.WriterCount,
		targetBytes: config.TargetBytes,
		startTime:   time.Now(),
		ycsbLogger:  config.YCSBLogger,
	}, nil
}

// Write writes documents from the channel to MongoDB
func (w *Writer) Write(ctx context.Context, docChan <-chan *model.CustomerDocument) error {
	eg, ctx := errgroup.WithContext(ctx)

	// Start multiple writer workers for parallel insertion
	for i := 0; i < w.writerCount; i++ {
		writerID := i
		eg.Go(func() error {
			return w.writeWorker(ctx, writerID, docChan)
		})
	}

	return eg.Wait()
}

// writeWorker is a worker that batches documents and writes them
func (w *Writer) writeWorker(ctx context.Context, writerID int, docChan <-chan *model.CustomerDocument) error {
	batch := make([]interface{}, 0, w.batchSize)
	ticker := time.NewTicker(100 * time.Millisecond) // Flush batch every 100ms if not full
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch before exiting
			if len(batch) > 0 {
				if err := w.flushBatch(ctx, batch); err != nil {
					return err
				}
			}
			return ctx.Err()

		case doc, ok := <-docChan:
			if !ok {
				// Channel closed, flush and exit
				if len(batch) > 0 {
					if err := w.flushBatch(ctx, batch); err != nil {
						return err
					}
				}
				return nil
			}

			batch = append(batch, doc)

			// Check if we've reached target
			if atomic.LoadInt64(&w.bytesWritten) >= w.targetBytes {
				// Flush batch and exit
				if len(batch) > 0 {
					if err := w.flushBatch(ctx, batch); err != nil {
						return err
					}
				}
				return nil
			}

			// Flush if batch is full
			if len(batch) >= w.batchSize {
				if err := w.flushBatch(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0] // Reset batch
			}

		case <-ticker.C:
			// Periodic flush to avoid holding documents too long
			if len(batch) > 0 {
				if err := w.flushBatch(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
}

// flushBatch writes a batch of documents to MongoDB
func (w *Writer) flushBatch(ctx context.Context, batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// Calculate actual bytes written
	var totalBytes int64
	for _, doc := range batch {
		bsonData, err := bson.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}
		totalBytes += int64(len(bsonData))
	}

	// Use InsertMany for better performance
	opts := options.InsertMany().SetOrdered(false) // Unordered for better performance

	// Record operation start time for YCSB logging
	startTime := time.Now()
	_, err := w.collection.InsertMany(ctx, batch, opts)
	latency := time.Since(startTime)

	success := err == nil
	if err != nil {
		// Log error but continue - some documents might have succeeded
		// In production, you might want more sophisticated error handling
	}

	// Record operation in YCSB logger if available
	if w.ycsbLogger != nil {
		// Record each document in the batch as a separate operation
		// Use average latency per document for more accurate metrics
		avgLatencyPerDoc := latency / time.Duration(len(batch))
		for i := 0; i < len(batch); i++ {
			w.ycsbLogger.RecordOperation("INSERT", avgLatencyPerDoc, success)
		}
	}

	// Update statistics
	atomic.AddInt64(&w.bytesWritten, totalBytes)
	atomic.AddInt64(&w.docsWritten, int64(len(batch)))

	// Update YCSB logger with bytes written
	if w.ycsbLogger != nil {
		w.ycsbLogger.UpdateBytesWritten(atomic.LoadInt64(&w.bytesWritten))
	}

	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	return nil
}

// GetStats returns current write statistics
func (w *Writer) GetStats() Stats {
	w.mu.RLock()
	defer w.mu.RUnlock()

	now := time.Now()
	docs := atomic.LoadInt64(&w.docsWritten)
	bytes := atomic.LoadInt64(&w.bytesWritten)

	elapsed := now.Sub(w.startTime).Seconds()
	var docsPerSec, bytesPerSec float64
	if elapsed > 0 {
		docsPerSec = float64(docs) / elapsed
		bytesPerSec = float64(bytes) / elapsed
	}

	return Stats{
		DocumentsWritten:   docs,
		BytesWritten:       bytes,
		DocumentsPerSecond: docsPerSec,
		BytesPerSecond:     bytesPerSec,
		StartTime:          w.startTime,
		LastUpdate:         now,
	}
}

// Stats represents write statistics
type Stats struct {
	DocumentsWritten   int64
	BytesWritten       int64
	DocumentsPerSecond float64
	BytesPerSecond     float64
	StartTime          time.Time
	LastUpdate         time.Time
}

// Close closes the MongoDB connection
func (w *Writer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Final stats will be written when the logger is closed
	return w.client.Disconnect(ctx)
}
