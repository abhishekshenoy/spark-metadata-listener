# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Maven-based Scala project that provides a pluggable metadata listener for Apache Spark ETL workflows. The main purpose is to capture detailed metadata about Spark jobs, including source/target information, file formats, metrics, filters, and column projections.

## Build and Development Commands

### Build and Compile
```bash
mvn clean compile              # Compile Scala sources
mvn clean package             # Build JAR file
mvn clean test                # Run tests
```

### Run Examples
```bash
mvn exec:java                 # Runs SparkETLMetadataListenerExample (default main class)
mvn exec:java -Dexec.mainClass="com.data.control.plane.ETLMetadataListenerUsage"
```

### Development with Proper JVM Settings
The project requires Java 11+ compatibility settings for Spark. These are already configured in the Maven plugins, but for manual execution:
```bash
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     -Xmx4g -cp target/classes [MainClass]
```

## Architecture

### Core Components

1. **SparkETLMetadataListener** (`com.data.control.plane.SparkETLMetadataListener`)
   - Main listener implementing both `SparkListener` and `QueryExecutionListener`
   - Thread-safe implementation using `ConcurrentHashMap` for:
      - `executionIdToQueryExecution`: Maps execution IDs to query executions
      - `jobToExecutionId`: Maps Spark job IDs to SQL execution IDs
      - `jobToStageMetrics`: Tracks metrics for each job's stages
      - `recentQueryExecutions`: Time-based query execution cache
   - Captures metadata from both physical and logical query plans
   - Provides callback mechanism for custom metadata handling
   - Includes debug mode for troubleshooting

### Data Models

The listener uses these data structures:
- `SourceNodeDetail`: Captures source information including table name, folder path, source type, projected columns, applied filters, and metrics before/after filtering
- `TargetNodeDetail`: Captures target information including table name, folder path, target type, and write metrics
- `ActionMetadata`: Complete metadata for an ETL action with action ID, type, timestamp, source/target nodes, execution time, and Spark job/stage IDs
- `MetricsDetail`: Detailed metrics including record count, bytes processed, file count, and partition count
- `StageMetrics`: Internal metrics tracking for individual Spark stages

### Registration Pattern

Listeners must be registered with SparkSession:
```scala
val listener = new SparkETLMetadataListener()
listener.registerWithSparkSession(spark)
listener.setMetadataCallback { metadata =>
   // Handle captured metadata
}
```

## Key Features

- **Multi-format Support**: Detects Parquet and ORC file sources/targets via `FileSourceScanExec` and write commands
- **Filter Extraction**: Captures data filters and partition filters from scan operations
- **Column Projection**: Tracks projected columns from scan operations
- **Comprehensive Metrics**: Records/bytes before and after filtering, file counts, partition counts
- **Thread Safety**: Concurrent data structures for multi-threaded Spark environments
- **Debug Mode**: Configurable debug logging for troubleshooting (`debugMode` flag)
- **Plan Correlation**: Associates Spark jobs with SQL execution plans using execution IDs
- **Fallback Detection**: Detects write operations even without full query execution context
- **Time-based Cleanup**: Automatic cleanup of cached query executions (1-minute retention)
- **Stage Metrics Aggregation**: Combines metrics across multiple Spark stages

## Example Usage

The project includes a comprehensive example in `SparkETLMetadataListenerExample` that demonstrates:

1. **Data Setup**: Creates sample sales and customer datasets in Parquet format
2. **ETL Operations**: Performs filtering, joins, and aggregations with metadata capture
3. **Multiple Writes**: Shows metadata capture for different write operations
4. **Callback Handling**: Demonstrates custom metadata processing

The example creates realistic data volumes (100K sales records, 1K customers) and performs operations that trigger various metadata capture scenarios including:
- File source scanning with filters and column projection
- Join operations across multiple datasets
- Aggregation and grouping operations
- Multiple target writes with different conditions

## Development Notes

- The project uses Spark 3.5.0, Scala 2.12.18, and Hadoop 3.3.4
- All listeners require careful handling of Spark's asynchronous execution model
- Metadata extraction relies on Spark's internal plan structures which may change between versions
- Test with both simple operations and complex multi-stage ETL workflows
- The listener includes automatic cleanup mechanisms to prevent memory leaks in long-running applications