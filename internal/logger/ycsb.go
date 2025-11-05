package logger

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// YCSBLogger implements YCSB-style logging
type YCSBLogger struct {
	file         *os.File
	mu           sync.Mutex
	operations   []Operation
	startTime    time.Time
	errorCount   int64
	successCount int64
	lastLogTime  time.Time
}

// Operation represents a single operation with timing
type Operation struct {
	Type      string
	LatencyUs int64 // Latency in microseconds
	Success   bool
}

// NewYCSBLogger creates a new YCSB logger that writes to a file
func NewYCSBLogger(filePath string) (*YCSBLogger, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	logger := &YCSBLogger{
		file:        file,
		startTime:   time.Now(),
		lastLogTime: time.Now(),
		operations:  make([]Operation, 0, 100000), // Pre-allocate for performance
	}

	// Write header
	logger.writeHeader()

	return logger, nil
}

// writeHeader writes the YCSB log header
func (l *YCSBLogger) writeHeader() {
	l.file.WriteString("YCSB Client 0.1\n")
	l.file.WriteString(fmt.Sprintf("Command line: gendata\n"))
	l.file.WriteString(fmt.Sprintf("Start time: %s\n", l.startTime.Format(time.RFC3339)))
	l.file.WriteString("\n")
}

// RecordOperation records an operation with its latency
func (l *YCSBLogger) RecordOperation(opType string, latency time.Duration, success bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	latencyUs := latency.Microseconds()
	l.operations = append(l.operations, Operation{
		Type:      opType,
		LatencyUs: latencyUs,
		Success:   success,
	})

	if success {
		l.successCount++
	} else {
		l.errorCount++
	}
}

// StartPeriodicLogging starts a goroutine that logs statistics every 10 seconds
func (l *YCSBLogger) StartPeriodicLogging(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.WriteStats()
		}
	}
}

// WriteStats writes YCSB-style statistics to the log file
func (l *YCSBLogger) WriteStats() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := time.Since(l.startTime)
	elapsedMs := elapsed.Milliseconds()

	// Calculate overall stats
	totalOps := int64(len(l.operations))
	if totalOps == 0 {
		return nil
	}

	// Write timestamp for this log entry
	l.file.WriteString(fmt.Sprintf("\n=== Stats at %s (elapsed: %s) ===\n",
		time.Now().Format(time.RFC3339),
		elapsed.Round(time.Second)))

	// Write overall stats
	l.file.WriteString(fmt.Sprintf("[OVERALL], RunTime(ms), %d\n", elapsedMs))

	throughput := float64(totalOps) / elapsed.Seconds()
	l.file.WriteString(fmt.Sprintf("[OVERALL], Throughput(ops/sec), %.2f\n", throughput))

	// Group operations by type
	opsByType := make(map[string][]Operation)
	for _, op := range l.operations {
		opsByType[op.Type] = append(opsByType[op.Type], op)
	}

	// Write stats for each operation type
	for opType, ops := range opsByType {
		l.writeOperationStats(opType, ops)
	}

	// Flush to ensure all data is written
	l.lastLogTime = time.Now()
	return l.file.Sync()
}

// writeOperationStats writes statistics for a specific operation type
func (l *YCSBLogger) writeOperationStats(opType string, ops []Operation) {
	if len(ops) == 0 {
		return
	}

	// Extract latencies
	latencies := make([]int64, len(ops))
	var totalLatency int64
	successCount := int64(0)
	errorCount := int64(0)

	for i, op := range ops {
		latencies[i] = op.LatencyUs
		totalLatency += op.LatencyUs
		if op.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate statistics
	avgLatency := float64(totalLatency) / float64(len(ops))
	minLatency := latencies[0]
	maxLatency := latencies[len(latencies)-1]

	// Calculate percentiles
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)
	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}

	p95Latency := latencies[p95Index]
	p99Latency := latencies[p99Index]

	// Write YCSB-style output
	l.file.WriteString(fmt.Sprintf("[%s], Operations, %d\n", opType, len(ops)))
	l.file.WriteString(fmt.Sprintf("[%s], AverageLatency(us), %.2f\n", opType, avgLatency))
	l.file.WriteString(fmt.Sprintf("[%s], MinLatency(us), %d\n", opType, minLatency))
	l.file.WriteString(fmt.Sprintf("[%s], MaxLatency(us), %d\n", opType, maxLatency))
	l.file.WriteString(fmt.Sprintf("[%s], 95thPercentileLatency(us), %d\n", opType, p95Latency))
	l.file.WriteString(fmt.Sprintf("[%s], 99thPercentileLatency(us), %d\n", opType, p99Latency))
	l.file.WriteString(fmt.Sprintf("[%s], Return=OK, Count, %d\n", opType, successCount))
	if errorCount > 0 {
		l.file.WriteString(fmt.Sprintf("[%s], Return=ERROR, Count, %d\n", opType, errorCount))
	}
}

// Close closes the log file
func (l *YCSBLogger) Close() error {
	// Write final stats
	l.WriteStats()
	l.file.WriteString("\n=== Final Statistics ===\n")
	return l.file.Close()
}
