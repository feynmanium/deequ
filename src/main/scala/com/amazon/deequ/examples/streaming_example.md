# Spark Streaming Support for Deequ

This guide explains how to use Deequ for continuous data quality monitoring on Spark Structured Streaming data.

## Overview

Deequ's streaming support enables you to:
- **Monitor data quality continuously** on streaming datasets
- **Maintain metrics across batches** using algebraic state aggregation
- **Detect quality issues in real-time** with immediate alerting
- **Track quality trends** over time with historical metrics storage

## Architecture

Deequ's streaming implementation leverages the existing algebraic state architecture:

1. **Micro-batch Processing**: Each streaming micro-batch is processed using the standard `AnalysisRunner`
2. **State Aggregation**: States from previous batches are loaded, combined with current batch states, and persisted
3. **Incremental Metrics**: Metrics reflect cumulative statistics across all processed batches
4. **Fault Tolerance**: Integration with Spark's checkpointing ensures recovery from failures

## Getting Started

### Basic Streaming Verification

Use `StreamingVerificationSuite` for high-level data quality monitoring:

```scala
import com.amazon.deequ.StreamingVerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.streaming.Trigger

// Create a streaming DataFrame (from Kafka, Kinesis, etc.)
val streamingData = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "events")
  .load()
  .selectExpr("CAST(value AS STRING) as json")
  .select(from_json($"json", schema).as("data"))
  .select("data.*")

// Define quality checks
val check = Check(CheckLevel.Error, "data quality")
  .isComplete("user_id")
  .isComplete("event_type")
  .hasCompleteness("email", _ >= 0.95)
  .isNonNegative("event_count")

// Start streaming verification
val query = StreamingVerificationSuite()
  .onData(streamingData)
  .addCheck(check)
  .withTrigger(Trigger.ProcessingTime("1 minute"))
  .withCheckpointLocation("/path/to/checkpoint")
  .onCheckFailure { (result, batchId) =>
    // Send alert when quality checks fail
    alertService.send(s"Quality check failed in batch $batchId")
  }
  .start()

query.awaitTermination()
```

### Streaming Analysis (Lower-Level API)

Use `StreamingAnalysisRunner` for more control over metrics computation:

```scala
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.StreamingAnalysisRunner
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository

// Define analyzers
val analyzers = Seq(
  Size(),
  Completeness("user_id"),
  Mean("session_duration"),
  ApproxCountDistinct("user_id"),
  Compliance("active_users", "last_active < current_timestamp()")
)

// Configure state persistence
val stateProvider = HdfsStateProvider(spark, "/path/to/state")
val metricsRepo = FileSystemMetricsRepository(spark, "/path/to/metrics")

// Start streaming analysis
val query = StreamingAnalysisRunner
  .onData(streamingData)
  .addAnalyzers(analyzers)
  .useStateProvider(stateProvider)
  .saveMetricsTo(
    metricsRepo,
    batchId => ResultKey(System.currentTimeMillis(), Map("batch" -> batchId.toString))
  )
  .withTrigger(Trigger.ProcessingTime("30 seconds"))
  .onBatchComplete { (metrics, batchId) =>
    // Process metrics for each batch
    println(s"Batch $batchId: ${metrics.allMetrics.size} metrics computed")
  }
  .start()

query.awaitTermination()
```

## State Management

### In-Memory State Provider

Best for testing and single-JVM scenarios:

```scala
val stateProvider = InMemoryStateProvider()
```

### HDFS State Provider

For production use with distributed storage:

```scala
val stateProvider = HdfsStateProvider(spark, "s3://bucket/deequ/state")
```

State is automatically:
- Loaded at the start of each batch
- Combined with newly computed state using algebraic operations
- Persisted after processing

## Metrics Repository Integration

Store computed metrics for historical analysis:

```scala
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.ResultKey

val metricsRepository = FileSystemMetricsRepository(spark, "s3://bucket/metrics")

StreamingVerificationSuite()
  .onData(streamingData)
  .addCheck(check)
  .saveMetricsTo(
    metricsRepository,
    batchId => ResultKey(
      dataSetDate = System.currentTimeMillis(),
      tags = Map(
        "batch" -> batchId.toString,
        "pipeline" -> "event-processing"
      )
    )
  )
  .start()
```

## Callbacks and Monitoring

### Success and Failure Callbacks

```scala
StreamingVerificationSuite()
  .onData(streamingData)
  .addCheck(check)
  .onCheckComplete { (result, batchId) =>
    // Executed for every batch
    metricsTracker.record("batches_processed", batchId)
  }
  .onCheckFailure { (result, batchId) =>
    // Executed only when checks fail
    result.checkResults.foreach { case (check, checkResult) =>
      checkResult.constraintResults
        .filter(_.status != ConstraintStatus.Success)
        .foreach { constraint =>
          alerting.sendAlert(constraint.constraint.toString, constraint.message)
        }
    }
  }
  .start()
```

### Custom Batch Processing

```scala
StreamingAnalysisRunner
  .onData(streamingData)
  .addAnalyzers(analyzers)
  .onBatchComplete { (analyzerContext, batchId) =>
    // Access specific metrics
    val sizeMetric = analyzerContext.metric(Size())
    val completenessMetric = analyzerContext.metric(Completeness("user_id"))

    // Log to monitoring system
    monitoring.gauge("total_records", sizeMetric.value.get)
    monitoring.gauge("user_id_completeness", completenessMetric.value.get)

    // Store in custom database
    database.insert(batchId, analyzerContext.allMetrics)
  }
  .start()
```

## Supported Analyzers

All standard Deequ analyzers work with streaming data:

### Scanning Analyzers
- `Size()` - Record count
- `Completeness(column)` - Non-null ratio
- `Mean(column)` - Average value
- `Sum(column)` - Sum of values
- `Minimum(column)` - Minimum value
- `Maximum(column)` - Maximum value
- `StandardDeviation(column)` - Standard deviation
- `ApproxCountDistinct(column)` - Approximate distinct count
- `Compliance(name, expression)` - Fraction matching condition

### Grouping Analyzers
- `Histogram(column)` - Value distribution
- `Uniqueness(columns)` - Uniqueness ratio
- `Distinctness(columns)` - Distinct value ratio
- `Entropy(column)` - Shannon entropy
- `MutualInformation(columns)` - Mutual information

## Configuration Options

### Trigger Modes

```scala
import org.apache.spark.sql.streaming.Trigger

// Process every minute
.withTrigger(Trigger.ProcessingTime("1 minute"))

// Process as fast as possible
.withTrigger(Trigger.Continuous("1 second"))

// Process once then stop
.withTrigger(Trigger.Once())
```

### Checkpointing

```scala
.withCheckpointLocation("/path/to/checkpoint")
```

Required for production deployments to enable fault tolerance.

### Query Naming

```scala
.withQueryName("deequ-quality-monitoring")
```

Helps identify queries in Spark UI and logs.

### Storage Level

```scala
import org.apache.spark.storage.StorageLevel

.useStorageLevel(StorageLevel.MEMORY_AND_DISK)
```

Controls caching behavior for grouping analyzers.

## Best Practices

### 1. Use Persistent State Storage

For production, use HDFS/S3 state provider:

```scala
val stateProvider = HdfsStateProvider(spark, "s3://bucket/state")
```

### 2. Set Appropriate Triggers

Balance between latency and throughput:

```scala
// For real-time monitoring
.withTrigger(Trigger.ProcessingTime("10 seconds"))

// For batch-like processing
.withTrigger(Trigger.ProcessingTime("5 minutes"))
```

### 3. Configure Checkpointing

Always set checkpoint location for fault tolerance:

```scala
.withCheckpointLocation("s3://bucket/checkpoints/deequ")
```

### 4. Monitor Query Health

Track query metrics:

```scala
.onBatchComplete { (context, batchId) =>
  query.lastProgress  // Access Spark streaming metrics
  query.status        // Check query status
}
```

### 5. Handle Check Failures

Implement alerting for quality issues:

```scala
.onCheckFailure { (result, batchId) =>
  // Send to monitoring system
  alerting.sendPageDutyAlert(severity = "high", message = result.toString)

  // Log detailed information
  logger.error(s"Quality check failed: $result")
}
```

## Example: Production Setup

```scala
import com.amazon.deequ.StreamingVerificationSuite
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.spark.sql.streaming.Trigger

// Configure components
val stateProvider = HdfsStateProvider(spark, "s3://data-quality/state")
val metricsRepo = FileSystemMetricsRepository(spark, "s3://data-quality/metrics")
val alertService = new AlertService()

// Define checks
val criticalChecks = Check(CheckLevel.Error, "critical")
  .isComplete("order_id")
  .isComplete("customer_id")
  .isNonNegative("amount")

val warningChecks = Check(CheckLevel.Warning, "warnings")
  .hasCompleteness("email", _ >= 0.95)
  .hasCompleteness("phone", _ >= 0.90)

// Start monitoring
val query = StreamingVerificationSuite()
  .onData(streamingData)
  .addCheck(criticalChecks)
  .addCheck(warningChecks)
  .useStateProvider(stateProvider)
  .saveMetricsTo(
    metricsRepo,
    batchId => ResultKey(
      dataSetDate = System.currentTimeMillis(),
      tags = Map("batch" -> batchId.toString, "env" -> "production")
    )
  )
  .withTrigger(Trigger.ProcessingTime("1 minute"))
  .withCheckpointLocation("s3://data-quality/checkpoints")
  .withQueryName("order-quality-monitor")
  .onCheckFailure { (result, batchId) =>
    alertService.send(
      severity = if (result.status == CheckStatus.Error) "critical" else "warning",
      message = s"Quality issues in batch $batchId",
      details = result.checkResults
    )
  }
  .start()

// Monitor query
while (query.isActive) {
  Thread.sleep(10000)
  println(s"Status: ${query.status}")
  println(s"Last Progress: ${query.lastProgress}")
}
```

## Troubleshooting

### Query Fails with "Not a streaming DataFrame"

Ensure your DataFrame is created from a streaming source:

```scala
// Correct
val df = spark.readStream.format("kafka")...

// Incorrect
val df = spark.read.format("kafka")...  // This is batch, not streaming
```

### State Not Persisting Across Batches

Verify state provider is configured:

```scala
.useStateProvider(HdfsStateProvider(spark, "/path/to/state"))
```

### Checkpoint Errors

Ensure checkpoint directory is writable and unique per query:

```scala
.withCheckpointLocation("s3://bucket/checkpoints/unique-query-name")
```

### Performance Issues

1. Adjust trigger interval for your workload
2. Use appropriate storage level for caching
3. Monitor Spark UI for bottlenecks

## Additional Resources

- [Deequ GitHub Repository](https://github.com/awslabs/deequ)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Algebraic States in Deequ](./algebraic_states_example.md)
