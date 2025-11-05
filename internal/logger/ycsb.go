package logger

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// YCSBLogger implements YCSB-style logging
type YCSBLogger struct {
	file            *os.File
	mu              sync.Mutex
	operations      []Operation
	startTime       time.Time
	errorCount      int64
	successCount    int64
	lastLogTime     time.Time
	lastOpCount     int64
	targetBytes     int64
	bytesWritten    int64
	workloadName    string
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
		file:         file,
		startTime:    time.Now(),
		lastLogTime:  time.Now(),
		operations:   make([]Operation, 0, 100000), // Pre-allocate for performance
		workloadName: "mongodb-data-generator",
	}

	// Write header
	logger.writeHeader()

	return logger, nil
}

// SetTargetBytes sets the target bytes for completion estimation
func (l *YCSBLogger) SetTargetBytes(targetBytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.targetBytes = targetBytes
}

// UpdateBytesWritten updates the bytes written for completion estimation
func (l *YCSBLogger) UpdateBytesWritten(bytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bytesWritten = bytes
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

// WriteStats writes YCSB-style statistics to the log file in the new format
func (l *YCSBLogger) WriteStats() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.startTime)
	elapsedSec := int64(elapsed.Seconds())

	// Calculate overall stats
	totalOps := int64(len(l.operations))
	if totalOps == 0 {
		return nil
	}

	// Calculate current ops/sec (operations in last 10 seconds)
	opsSinceLastLog := totalOps - l.lastOpCount
	periodDuration := now.Sub(l.lastLogTime).Seconds()
	if periodDuration < 1 {
		periodDuration = 1 // Avoid division by zero
	}
	currentOpsPerSec := float64(opsSinceLastLog) / periodDuration

	// Estimate completion time
	var estCompletion string
	if l.targetBytes > 0 && l.bytesWritten < l.targetBytes {
		remainingBytes := l.targetBytes - l.bytesWritten
		bytesPerSec := float64(l.bytesWritten) / elapsed.Seconds()
		if bytesPerSec > 0 {
			remainingSec := float64(remainingBytes) / bytesPerSec
			estCompletion = formatDuration(time.Duration(remainingSec) * time.Second)
		} else {
			estCompletion = "unknown"
		}
	} else {
		estCompletion = "N/A"
	}

	// Format timestamp: [2025/10/23 15:02:50.756]
	timestamp := now.Format("[2006/01/02 15:04:05.000]")

	// Format second timestamp: 2025-10-23 22:02:50:656
	timestamp2 := now.Format("2006-01-02 15:04:05:000")

	// Group operations by type
	opsByType := make(map[string][]Operation)
	for _, op := range l.operations {
		opsByType[op.Type] = append(opsByType[op.Type], op)
	}

	// Build operation stats strings
	var opStatsStrings []string
	for opType, ops := range opsByType {
		opStatsStr := l.formatOperationStatsInline(opType, ops)
		opStatsStrings = append(opStatsStrings, opStatsStr)
	}

	// Write single-line progress report
	line := fmt.Sprintf("%s [info   ] [%s] %s %d sec: %d operations; %.1f current ops/sec; est completion in %s",
		timestamp, l.workloadName, timestamp2, elapsedSec, totalOps, currentOpsPerSec, estCompletion)

	// Append operation stats
	for _, opStat := range opStatsStrings {
		line += " " + opStat
	}

	l.file.WriteString(line + "\n")

	// Flush to ensure all data is written
	l.lastLogTime = now
	l.lastOpCount = totalOps
	return l.file.Sync()
}

// formatOperationStatsInline formats operation statistics in a single line
func (l *YCSBLogger) formatOperationStatsInline(opType string, ops []Operation) string {
	if len(ops) == 0 {
		return fmt.Sprintf("[%s: Count=0]", opType)
	}

	// Extract latencies
	latencies := make([]int64, len(ops))
	var totalLatency int64
	successCount := int64(0)

	for i, op := range ops {
		latencies[i] = op.LatencyUs
		totalLatency += op.LatencyUs
		if op.Success {
			successCount++
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
	p90Index := int(float64(len(latencies)) * 0.90)
	p99Index := int(float64(len(latencies)) * 0.99)
	p999Index := int(float64(len(latencies)) * 0.999)
	p9999Index := int(float64(len(latencies)) * 0.9999)

	if p90Index >= len(latencies) {
		p90Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}
	if p999Index >= len(latencies) {
		p999Index = len(latencies) - 1
	}
	if p9999Index >= len(latencies) {
		p9999Index = len(latencies) - 1
	}

	p90Latency := latencies[p90Index]
	p99Latency := latencies[p99Index]
	p999Latency := latencies[p999Index]
	p9999Latency := latencies[p9999Index]

	// Format as: [INSERT: Count=..., Max=..., Min=..., Avg=..., 90=..., 99=..., 99.9=..., 99.99=...]
	return fmt.Sprintf("[%s: Count=%d, Max=%d, Min=%d, Avg=%.2f, 90=%d, 99=%d, 99.9=%d, 99.99=%d]",
		opType, len(ops), maxLatency, minLatency, avgLatency,
		p90Latency, p99Latency, p999Latency, p9999Latency)
}

// formatDuration formats a duration in a human-readable format like "1 day 5 hours" or "2 hours 30 minutes"
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "N/A"
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day", days))
		if days > 1 {
			parts[len(parts)-1] += "s"
		}
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour", hours))
		if hours > 1 {
			parts[len(parts)-1] += "s"
		}
	}
	if minutes > 0 && days == 0 {
		parts = append(parts, fmt.Sprintf("%d minute", minutes))
		if minutes > 1 {
			parts[len(parts)-1] += "s"
		}
	}

	if len(parts) == 0 {
		return "less than a minute"
	}

	return strings.Join(parts, " ")
}

// Close closes the log file and writes final statistics
func (l *YCSBLogger) Close() error {
	// Write final statistics summary in multi-line format
	l.WriteFinalStats()
	return l.file.Close()
}

// WriteFinalStats writes comprehensive final statistics in multi-line YCSB format
func (l *YCSBLogger) WriteFinalStats() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := time.Since(l.startTime)
	elapsedMs := elapsed.Milliseconds()
	totalOps := int64(len(l.operations))

	if totalOps == 0 {
		return nil
	}

	// Calculate overall throughput
	throughput := float64(totalOps) / elapsed.Seconds()

	// Write overall stats
	l.file.WriteString(fmt.Sprintf("[OVERALL], RunTime(ms), %d\n", elapsedMs))
	
	// Format timestamp for final stats lines
	timestamp := time.Now().Format("[2006/01/02 15:04:05.000]")
	
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [OVERALL], Throughput(ops/sec), %.15f\n",
		timestamp, l.workloadName, throughput))

	// Group operations by type
	opsByType := make(map[string][]Operation)
	for _, op := range l.operations {
		opsByType[op.Type] = append(opsByType[op.Type], op)
	}

	// Write stats for each operation type
	for opType, ops := range opsByType {
		l.writeFinalOperationStats(opType, ops, timestamp)
	}

	return l.file.Sync()
}

// writeFinalOperationStats writes comprehensive statistics for an operation type in multi-line format
func (l *YCSBLogger) writeFinalOperationStats(opType string, ops []Operation, timestamp string) {
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
	p50Index := int(float64(len(latencies)) * 0.50)
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)
	p999Index := int(float64(len(latencies)) * 0.999)
	p9999Index := int(float64(len(latencies)) * 0.9999)
	p99999Index := int(float64(len(latencies)) * 0.99999)

	if p50Index >= len(latencies) {
		p50Index = len(latencies) - 1
	}
	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}
	if p999Index >= len(latencies) {
		p999Index = len(latencies) - 1
	}
	if p9999Index >= len(latencies) {
		p9999Index = len(latencies) - 1
	}
	if p99999Index >= len(latencies) {
		p99999Index = len(latencies) - 1
	}

	p50Latency := latencies[p50Index]
	p95Latency := latencies[p95Index]
	p99Latency := latencies[p99Index]
	p999Latency := latencies[p999Index]
	p9999Latency := latencies[p9999Index]
	p99999Latency := latencies[p99999Index]

	// Write multi-line statistics
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], Operations, %d\n",
		timestamp, l.workloadName, opType, len(ops)))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], AverageLatency(us), %.15f\n",
		timestamp, l.workloadName, opType, avgLatency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], MinLatency(us), %d\n",
		timestamp, l.workloadName, opType, minLatency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], MaxLatency(us), %d\n",
		timestamp, l.workloadName, opType, maxLatency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 50thPercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p50Latency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 95thPercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p95Latency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 99thPercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p99Latency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 99.9PercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p999Latency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 99.99PercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p9999Latency))
	l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], 99.999PercentileLatency(us), %d\n",
		timestamp, l.workloadName, opType, p99999Latency))
	if successCount > 0 {
		l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], Return=OK, Count, %d\n",
			timestamp, l.workloadName, opType, successCount))
	}
	if errorCount > 0 {
		l.file.WriteString(fmt.Sprintf("%s [info   ] [%s] [%s], Return=ERROR, Count, %d\n",
			timestamp, l.workloadName, opType, errorCount))
	}
}
