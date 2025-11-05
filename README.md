# MongoDB Data Generator

A Go tool for generating large volumes of realistic test data to MongoDB Atlas clusters.

## Features

- **Flexible Document Sizes**: Support for 2KB, 4KB, 8KB, 16KB, 32KB, and 64KB documents
- **Intelligent Sizing**: Automatically selects optimal document size based on target data volume
- **Realistic Data**: Uses Faker library to generate meaningful customer/order documents with nested structures
- **Concurrent Processing**: Multiple generator workers and MongoDB writers for maximum throughput
- **Real-time Metrics**: Progress reporting with docs/sec and MB/s statistics

## Requirements

- Go 1.21 or later
- MongoDB Atlas cluster (or any MongoDB instance)
- Network access to MongoDB cluster from client VM

## Configuration

**Important**: Never commit MongoDB connection strings with credentials to version control. Use environment variables instead:

```bash
export MONGODB_URI=
```

## Installation

### Download Pre-built Binaries

Download the latest release binary for your platform from [GitHub Releases](https://github.com/meticulous-dft/mongodb-data-generator/releases):

```bash
# For Linux x86_64
curl -L -o gendata https://github.com/meticulous-dft/mongodb-data-generator/releases/latest/download/gendata-linux-amd64
chmod +x gendata

# For Linux ARM64
curl -L -o gendata https://github.com/meticulous-dft/mongodb-data-generator/releases/latest/download/gendata-linux-arm64
chmod +x gendata
```

### Build from Source

```bash
# Clone the repository
git clone https://github.com/meticulous-dft/mongodb-data-generator.git
cd mongodb-data-generator

# Build the tool
make build

# Or build manually
go build -o bin/gendata ./cmd/gendata
```

## Usage

### Basic Usage

```bash
./bin/gendata \
  --connection "$MONGODB_URI" \
  --size 1TB
```

### Advanced Options

```bash
./bin/gendata \
  --connection "$MONGODB_URI" \
  --database testdb \
  --collection customers \
  --size 1TB \
  --doc-size 8KB \
  --workers 20 \
  --writers 10 \
  --batch-size 2000 \
  --log-file ycsb.log \
  --verbose
```

### Command Line Options

- `--connection` (required): MongoDB connection string (use `$MONGODB_URI` environment variable or provide connection string)
- `--database`: Database name (default: `testdb`)
- `--collection`: Collection name (default: `customers`)
- `--size`: Target data size (e.g., `1TB`, `500GB`, `32TB`)
- `--doc-size`: Document size (`2KB`, `4KB`, `8KB`, `16KB`, `32KB`, `64KB`, or `auto`)
  - **Auto mode scaling**: 
    - `< 100GB`: 2KB documents
    - `< 1TB`: 4KB documents
    - `< 2TB`: 8KB documents
    - `< 4TB`: 16KB documents
    - `< 8TB`: 32KB documents
    - `>= 8TB`: 64KB documents
- `--workers`: Number of generator workers (default: `CPU count * 2`)
- `--writers`: Number of MongoDB writer workers (default: `CPU count`)
- `--batch-size`: Batch size for MongoDB writes (default: `2000`)
- `--verbose`: Enable verbose logging
- `--log-file`: Path to YCSB-style log file (default: `ycsb.log`)

### Performance Tuning

1. **Use larger documents**: 8KB-64KB documents provide better throughput
2. **Increase workers**: Scale with available CPU cores
3. **Use multiple writers**: Allows parallel MongoDB connections
4. **Larger batch sizes**: Reduces network round-trips (2000-5000 recommended)
5. **Regional proximity**: Run from a VM in the same region as your Atlas cluster
6. **Network**: Ensure sufficient network bandwidth

### Document Structure

Generated documents follow a customer/order schema with:
- Customer information (name, email, phone, etc.)
- Multiple addresses (home, work, shipping, billing)
- Payment methods (credit cards, PayPal, etc.)
- Order history with line items
- Metadata, notes, and tags
- Padding to reach exact target document size

The document structure scales with target size to ensure meaningful data is the majority (>80%) of each document, with padding limited to <20%. For example:
- **2KB documents**: Minimal structure (customer + 1 address + 1 payment, no orders)
- **64KB documents**: Full structure with 12-14 orders, 8-15 line items per order, extended metadata (30-50 entries), and comprehensive notes/tags

### Compression Settings

For performance testing scenarios where storage size should match logical size, the tool automatically disables compression:

1. **Network Compression**: Disabled by appending `compressors=disabled` to the MongoDB connection string
2. **Storage Compression**: Disabled by creating collections with WiredTiger `block_compressor=none` setting

These settings ensure that:
- Logical data size matches storage size (important for volume snapshotting and initial sync performance testing)
- No compression overhead during data generation
- Accurate representation of actual storage requirements

**Note**: If the collection already exists, the tool will attempt to create it with these settings. If creation fails (e.g., due to permissions or existing collection), the tool will use the existing collection as-is.


## Performance Benchmarking

The tool includes built-in progress reporting showing:
- Documents generated per second
- Bytes written per second
- Total data written
- Estimated time to completion

Example output:
```
[Gen: 125000 docs, 1950.45 MB/s] [Write: 125000 docs, 1950.45 MB/s] [Total: 2.05 GB]
```

### YCSB-Style Logging

The tool generates YCSB (Yahoo! Cloud Serving Benchmark) style logs to a file (default: `ycsb.log`). Statistics are logged every 10 seconds during execution, showing cumulative metrics from the start. The log includes:

- Overall runtime and throughput
- Operation counts (INSERT operations)
- Latency statistics (average, min, max, 95th percentile, 99th percentile)
- Success and error counts

Example log output (logged every 10 seconds):
```
=== Stats at 2024-01-15T10:30:00Z (elapsed: 30s) ===
[OVERALL], RunTime(ms), 30000
[OVERALL], Throughput(ops/sec), 12500.50
[INSERT], Operations, 375015
[INSERT], AverageLatency(us), 125.50
[INSERT], MinLatency(us), 45
[INSERT], MaxLatency(us), 2500
[INSERT], 95thPercentileLatency(us), 350
[INSERT], 99thPercentileLatency(us), 850
[INSERT], Return=OK, Count, 375015
```

The log file is written periodically (every 10 seconds) and finalized with a summary on completion or shutdown.

## Development

### Building

```bash
make build
```

### Running Tests

```bash
make test
```

### Code Quality

```bash
make vet
make lint  # Requires golangci-lint
```
