package generator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/meticulous-dft/mongodb-data-generator/internal/model"
	"golang.org/x/sync/errgroup"
)

// Service handles document generation with high concurrency
type Service struct {
	docGenerator *model.Generator
	workerCount  int
	batchSize    int
	docChan      chan *model.CustomerDocument
	targetBytes  int64
	bytesGenerated int64
	docsGenerated   int64
	mu              sync.RWMutex
	startTime       time.Time
}

// Config holds generator service configuration
type Config struct {
	DocumentSize DocumentSize
	WorkerCount  int
	BatchSize    int
	TargetBytes  int64
}

// DocumentSize is an alias for model.DocumentSize
type DocumentSize = model.DocumentSize

// NewService creates a new generator service
func NewService(config Config) *Service {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 10 // Default to 10 workers
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000 // Default batch size
	}
	
	docGenerator := model.NewGenerator(config.DocumentSize)
	
	return &Service{
		docGenerator: docGenerator,
		workerCount:  config.WorkerCount,
		batchSize:    config.BatchSize,
		docChan:      make(chan *model.CustomerDocument, config.BatchSize*2),
		targetBytes:  config.TargetBytes,
		startTime:    time.Now(),
	}
}

// Generate starts generating documents and sends them to the channel
func (s *Service) Generate(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	
	// Start worker goroutines
	for i := 0; i < s.workerCount; i++ {
		workerID := i
		eg.Go(func() error {
			return s.worker(ctx, workerID)
		})
	}
	
	// Monitor and close channel when target is reached
	eg.Go(func() error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if atomic.LoadInt64(&s.bytesGenerated) >= s.targetBytes {
					close(s.docChan)
					return nil
				}
			}
		}
	})
	
	// Wait for all workers to complete
	return eg.Wait()
}

// worker generates documents and sends them to the channel
func (s *Service) worker(ctx context.Context, workerID int) error {
	for {
		// Check if we've reached target
		if atomic.LoadInt64(&s.bytesGenerated) >= s.targetBytes {
			return nil
		}
		
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Generate document
			doc, err := s.docGenerator.Generate()
			if err != nil {
				return err
			}
			
			// Estimate document size (we'll get actual size from BSON later)
			// For now, use target size as approximation
			docSize := int64(s.docGenerator.TargetSize())
			
			// Check again before sending
			currentBytes := atomic.LoadInt64(&s.bytesGenerated)
			if currentBytes+docSize > s.targetBytes {
				// We're close to target, don't send this one
				return nil
			}
			
			// Send document to channel (non-blocking check first)
			select {
			case s.docChan <- doc:
				atomic.AddInt64(&s.bytesGenerated, docSize)
				atomic.AddInt64(&s.docsGenerated, 1)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Documents returns the channel for consuming generated documents
func (s *Service) Documents() <-chan *model.CustomerDocument {
	return s.docChan
}

// GetStats returns current statistics
func (s *Service) GetStats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	now := time.Now()
	docs := atomic.LoadInt64(&s.docsGenerated)
	bytes := atomic.LoadInt64(&s.bytesGenerated)
	
	elapsed := now.Sub(s.startTime).Seconds()
	var docsPerSec, bytesPerSec float64
	if elapsed > 0 {
		docsPerSec = float64(docs) / elapsed
		bytesPerSec = float64(bytes) / elapsed
	}
	
	return Stats{
		DocumentsGenerated: docs,
		BytesGenerated:     bytes,
		DocumentsPerSecond: docsPerSec,
		BytesPerSecond:     bytesPerSec,
		StartTime:          s.startTime,
		LastUpdate:         now,
	}
}

// Stats represents generation statistics
type Stats struct {
	DocumentsGenerated int64
	BytesGenerated     int64
	DocumentsPerSecond float64
	BytesPerSecond     float64
	StartTime          time.Time
	LastUpdate         time.Time
}
