# MongoDB Data Generator

A high-performance Go tool for generating large volumes of realistic test data to MongoDB Atlas clusters. Designed to test MongoDB load, volume snapshotting, and initial sync performance.

## Features

- **High Throughput**: Optimized for generating 1 TB of data in less than 30 minutes
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
export MONGODB_URI="mongodb+srv://<username>:<password>@<cluster>.mongodb.net/"
```

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd data-generator

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

Or with an explicit connection string:
```bash
./bin/gendata \
  --connection "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/" \
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
  --verbose
```

### Command Line Options

- `--connection` (required): MongoDB connection string (use `$MONGODB_URI` environment variable or provide connection string)
- `--database`: Database name (default: `testdb`)
- `--collection`: Collection name (default: `customers`)
- `--size`: Target data size (e.g., `1TB`, `500GB`, `32TB`)
- `--doc-size`: Document size (`2KB`, `4KB`, `8KB`, `16KB`, `32KB`, `64KB`, or `auto`)
- `--workers`: Number of generator workers (default: `CPU count * 2`)
- `--writers`: Number of MongoDB writer workers (default: `CPU count`)
- `--batch-size`: Batch size for MongoDB writes (default: `2000`)
- `--verbose`: Enable verbose logging

### Performance Tuning

For optimal performance targeting 1 TB in 30 minutes (550+ MB/s):

1. **Use larger documents**: 8KB-64KB documents provide better throughput
2. **Increase workers**: Scale with available CPU cores
3. **Use multiple writers**: Allows parallel MongoDB connections
4. **Larger batch sizes**: Reduces network round-trips (2000-5000 recommended)
5. **Regional proximity**: Run from a VM in the same region as your Atlas cluster
6. **Network**: Ensure sufficient network bandwidth

### Recommended Settings for 1 TB in 30 Minutes

```bash
./bin/gendata \
  --connection "$MONGODB_URI" \
  --size 1TB \
  --doc-size 16KB \
  --workers 16 \
  --writers 8 \
  --batch-size 3000
```

### Document Structure

Generated documents follow a customer/order schema with:
- Customer information (name, email, phone, etc.)
- Multiple addresses (home, work, shipping, billing)
- Payment methods (credit cards, PayPal, etc.)
- Order history with line items
- Metadata, notes, and tags
- Padding to reach exact target document size

## MongoDB Atlas Readiness Checklist

Before running large data generation:

- [ ] Network peering configured (if using private network)
- [ ] IP whitelist includes client VM IP
- [ ] Sufficient cluster resources (CPU, memory, IOPS)
- [ ] Write concern configured appropriately (W:1 for maximum throughput)
- [ ] No indexes on collection (for initial load, add indexes after)
- [ ] Consider sharding for very large datasets (32TB+)

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

## Troubleshooting

### Slow Performance

- Check network latency to MongoDB Atlas
- Verify you're in the same region as the cluster
- Increase batch size and worker counts
- Use larger document sizes
- Check MongoDB Atlas cluster metrics for bottlenecks

### Connection Errors

- Verify connection string format
- Check IP whitelist settings
- Ensure network connectivity
- Verify authentication credentials

### Memory Usage

- Reduce batch size if experiencing memory pressure
- Reduce number of workers
- Monitor system resources during generation

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

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]

