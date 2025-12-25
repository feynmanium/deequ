# DQX-Style Features for Deequ Streaming

This guide covers Databricks DQX-inspired features implemented in Deequ for Spark Structured Streaming, enabling enterprise-grade data quality management patterns.

## Overview

Deequ now supports DQX-style patterns for:
- **Quarantine functionality** - Split data into valid and invalid streams
- **Row-level quality marking** - Track which checks failed at row level
- **Separate data sinks** - Route clean data to production, quarantined data for review
- **Real-time quality monitoring** - Continuous quality checks on streaming data
- **State aggregation** - Maintain cumulative metrics across batches

## Key Concepts

### Quarantine Pattern

The quarantine pattern separates data into two streams:
1. **Valid Data**: Rows passing all critical quality checks → Production tables
2. **Quarantined Data**: Rows failing quality checks → Review/remediation tables

This is the core pattern used in Databricks DQX for real-time data quality management.

### Row-Level Checks

Unlike aggregate checks (e.g., "completeness >= 95%"), row-level checks evaluate each row:
- Mark individual rows as valid or invalid
- Add metadata columns showing which checks failed
- Enable per-row routing decisions

### Criticality Levels

- **Error**: Critical quality issues - quarantine the data
- **Warning**: Quality concerns - log but allow data through
- **Info**: Informational metrics - no data blocking

## Using Quarantine in Streaming

### Basic Pattern

```scala
import com.amazon.deequ.quarantine.{QuarantineEngine, QuarantineConfig, RowLevelConfig}
import com.amazon.deequ.checks.{Check, CheckLevel}

// Define quality checks
val checks = Check(CheckLevel.Error, "critical checks")
  .isComplete("user_id")
  .isComplete("transaction_id")
  .isNonNegative("amount")

// Process streaming batches with quarantine
streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    // Apply checks and split data
    val result = QuarantineEngine.runWithQuarantine(
      data = batchDF,
      checks = Seq(checks),
      rowLevelConfig = RowLevelConfig(markInvalidRows = true),
      quarantineConfig = QuarantineConfig(quarantineOnError = true)
    )

    // Write to separate sinks
    result.validData
      .write.format("delta")
      .mode("append")
      .saveAsTable("prod.clean_transactions")

    result.quarantinedData
      .write.format("delta")
      .mode("append")
      .saveAsTable("prod.quarantine_transactions")
  }
  .start()
```

### With State Aggregation

```scala
val stateProvider = InMemoryStateProvider()

streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    val result = QuarantineEngine.runWithQuarantine(
      data = batchDF,
      checks = Seq(checks),
      aggregateWith = Some(stateProvider),    // Load previous state
      saveStatesWith = Some(stateProvider),   // Save updated state
      quarantineConfig = QuarantineConfig(quarantineOnError = true)
    )

    // Metrics are cumulative across all batches
    println(s"Total processed: ${result.verificationResult.metrics.size}")

    // Write to sinks...
  }
  .start()
```

## Configuration Options

### RowLevelConfig

Controls row-level marking behavior:

```scala
RowLevelConfig(
  markInvalidRows = true,              // Add marker column for invalid rows
  invalidMarkerColumn = "_is_quarantined",  // Name of marker column
  checkFailureColumns = true,          // Add columns showing which checks failed
  dropCheckColumns = false             // Keep check result columns in output
)
```

### QuarantineConfig

Controls quarantine behavior:

```scala
QuarantineConfig(
  quarantineOnError = true,    // Quarantine rows failing Error-level checks
  quarantineOnWarning = false, // Don't quarantine Warning-level failures
  addMetadataColumns = true,   // Add metadata (timestamp, check names, etc.)
  metadataPrefix = "_deequ_"   // Prefix for metadata columns
)
```

## Advanced Patterns

### Separate Sinks with Alerting

```scala
streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    val result = QuarantineEngine.runWithQuarantine(
      data = batchDF,
      checks = Seq(criticalChecks, warningChecks)
    )

    // Write valid data to production
    result.validData
      .write.format("delta").mode("append")
      .option("mergeSchema", "true")
      .saveAsTable("production.clean_data")

    // Write quarantined data for review
    if (!result.quarantinedData.isEmpty) {
      result.quarantinedData
        .write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("production.quarantine")

      // Send alert
      alertService.send(
        severity = "high",
        message = s"Batch $batchId: ${result.quarantinedData.count()} records quarantined",
        details = result.verificationResult
      )
    }
  }
  .start()
```

### Using get_valid() and get_invalid()

For DataFrames with row-level check result columns:

```scala
import com.amazon.deequ.quarantine.QuarantineEngine

// Assuming df has check result columns from a previous verification
val validDF = QuarantineEngine.getValid(
  data = df,
  quarantineOnError = true,
  dropCheckColumns = true  // Remove check metadata from output
)

val invalidDF = QuarantineEngine.getInvalid(
  data = df,
  errorOnly = true,
  dropCheckColumns = false  // Keep check metadata for debugging
)

// Write to different locations
validDF.write.saveAsTable("prod.clean")
invalidDF.write.saveAsTable("prod.quarantine")
```

### Multi-Stage Quality Pipeline

```scala
// Stage 1: Bronze layer - collect all data with metadata
val bronzeQuery = streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    val result = QuarantineEngine.runWithQuarantine(
      data = batchDF,
      checks = Seq(schemaChecks),
      rowLevelConfig = RowLevelConfig(
        markInvalidRows = true,
        dropCheckColumns = false  // Keep all check columns
      )
    )

    // Write everything to bronze (with quality markers)
    result.validData.union(result.quarantinedData)
      .write.format("delta").mode("append")
      .saveAsTable("bronze.raw_data")
  }
  .start()

// Stage 2: Silver layer - only clean data
val silverQuery = spark.readStream
  .format("delta")
  .table("bronze.raw_data")
  .writeStream
  .foreachBatch { (batchDF, batchId) =>
    // Filter to valid records only
    val cleanDF = QuarantineEngine.getValid(
      data = batchDF,
      dropCheckColumns = true
    )

    // Apply additional checks
    val result = QuarantineEngine.runWithQuarantine(
      data = cleanDF,
      checks = Seq(businessRules)
    )

    result.validData
      .write.format("delta").mode("append")
      .saveAsTable("silver.clean_data")

    result.quarantinedData
      .write.format("delta").mode("append")
      .saveAsTable("silver.business_rule_failures")
  }
  .start()
```

## Monitoring and Alerting

### Track Quality Metrics Over Time

```scala
val metricsRepository = FileSystemMetricsRepository(spark, "s3://metrics/deequ")

streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    val result = QuarantineEngine.runWithQuarantine(
      data = batchDF,
      checks = Seq(checks)
    )

    // Save metrics for historical analysis
    val resultKey = ResultKey(
      dataSetDate = System.currentTimeMillis(),
      tags = Map(
        "batch" -> batchId.toString,
        "pipeline" -> "transaction-processing",
        "environment" -> "production"
      )
    )

    val analyzerContext = AnalyzerContext(result.verificationResult.metrics)
    metricsRepository.save(resultKey, analyzerContext)

    // Track quarantine rate
    val totalRecords = batchDF.count()
    val quarantinedRecords = result.quarantinedData.count()
    val quarantineRate = quarantinedRecords.toDouble / totalRecords

    metricsTracker.gauge("quarantine_rate", quarantineRate)
    metricsTracker.gauge("quarantine_count", quarantinedRecords)

    // Alert if quarantine rate exceeds threshold
    if (quarantineRate > 0.05) {  // More than 5% quarantined
      alertService.send(
        severity = "critical",
        message = s"High quarantine rate: ${(quarantineRate * 100).formatted("%.2f")}%"
      )
    }
  }
  .start()
```

### Custom Quality Dashboards

```scala
// Query historical metrics
val metrics = metricsRepository.load()
  .withTagFilter(tags => tags("pipeline") == "transaction-processing")
  .get()

// Analyze trends
metrics.foreach { case (resultKey, analyzerContext) =>
  val timestamp = resultKey.dataSetDate
  val completeness = analyzerContext.metric(Completeness("email"))
  val size = analyzerContext.metric(Size())

  // Store in dashboard database
  dashboardDB.insert(timestamp, completeness, size)
}
```

## Comparison with Databricks DQX

### Feature Parity

| Feature | Databricks DQX | Deequ Streaming |
|---------|---------------|-----------------|
| Streaming support | ✓ | ✓ |
| Quarantine/split | ✓ | ✓ |
| Row-level checks | ✓ | ✓ (via row-level constraints) |
| Criticality levels | ✓ (error/warn) | ✓ (error/warn/info) |
| Metadata columns | ✓ | ✓ |
| State aggregation | ✓ | ✓ (algebraic states) |
| YAML config | ✓ | Planned |
| Auto-profiling | ✓ | Planned |
| Rule generation | ✓ | Planned |
| DLT integration | ✓ (native) | Via custom patterns |

### API Comparison

#### Databricks DQX
```python
# DQX pattern
dq_engine = DQEngine(workspace_client)
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(
    input_df, checks
)
```

#### Deequ Streaming (DQX-style)
```scala
// Deequ equivalent
val result = QuarantineEngine.runWithQuarantine(
  data = inputDF,
  checks = checks
)
val validDF = result.validData
val quarantineDF = result.quarantinedData
```

## Best Practices

### 1. Use Appropriate Criticality Levels

```scala
// Critical business rules → Error (quarantine)
val criticalChecks = Check(CheckLevel.Error, "critical")
  .isComplete("transaction_id")
  .isComplete("customer_id")
  .isNonNegative("amount")

// Data quality warnings → Warning (log but don't block)
val warningChecks = Check(CheckLevel.Warning, "warnings")
  .hasCompleteness("email", _ >= 0.90)
  .hasCompleteness("phone", _ >= 0.80)
```

### 2. Add Metadata for Debugging

```scala
QuarantineConfig(
  addMetadataColumns = true,  // Always true for quarantine data
  metadataPrefix = "_deequ_"
)
```

Quarantined data will include:
- `_deequ_checked_at`: Timestamp when checks were applied
- `_deequ_checks`: Which checks were run
- `_is_quarantined`: Boolean marker
- Individual check result columns

### 3. Monitor Quarantine Rates

```scala
streamingDF.writeStream
  .foreachBatch { (batchDF, batchId) =>
    val result = QuarantineEngine.runWithQuarantine(...)

    val total = result.validData.count() + result.quarantinedData.count()
    val quarantineRate = result.quarantinedData.count().toDouble / total

    // Track in monitoring system
    metrics.gauge("quality.quarantine_rate", quarantineRate)

    // Alert if exceeds threshold
    if (quarantineRate > 0.10) {
      alerting.sendAlert("High quarantine rate", quarantineRate)
    }
  }
  .start()
```

### 4. Implement Quarantine Review Process

```scala
// Daily job to review quarantined data
val quarantinedData = spark.read
  .format("delta")
  .table("prod.quarantine")
  .filter(col("_deequ_checked_at") >= current_date())

// Aggregate by check failure type
quarantinedData
  .groupBy("_deequ_checks")
  .count()
  .orderBy(desc("count"))
  .show()

// Fix common issues and reprocess
val fixedData = quarantinedData
  .filter(/* specific issue */)
  .transform(fixFunction)

// Re-run checks on fixed data
val recheck = QuarantineEngine.runWithQuarantine(
  data = fixedData,
  checks = originalChecks
)

// Promote to production if now valid
recheck.validData
  .write.format("delta").mode("append")
  .saveAsTable("prod.clean_data")
```

### 5. Use Checkpoints for Fault Tolerance

```scala
streamingDF.writeStream
  .foreachBatch(quarantineBatchProcessor)
  .option("checkpointLocation", "s3://checkpoints/quarantine-pipeline")
  .trigger(Trigger.ProcessingTime("1 minute"))
  .start()
```

## Additional Resources

- [Deequ Streaming Guide](./streaming_example.md)
- [Databricks DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Data Quality Management for Spark Streaming](https://medium.com/totalenergies-digital-factory/data-quality-management-for-spark-streaming-workloads-on-databricks-82d46f6674ec)
- [Getting Started with DQX](https://community.databricks.com/t5/databrickstv/getting-started-with-dqx/ba-p/113662)

## Summary

Deequ's DQX-style features enable enterprise-grade streaming data quality:

✅ **Quarantine Pattern** - Separate valid and invalid data streams
✅ **Row-Level Checks** - Mark and track individual row quality
✅ **Real-time Monitoring** - Continuous quality checks on streaming data
✅ **State Aggregation** - Cumulative metrics across batches
✅ **Flexible Configuration** - Control quarantine behavior and metadata
✅ **Production Ready** - Fault-tolerant with checkpointing support

Use these patterns to build robust, production-grade streaming data pipelines with comprehensive quality management.
