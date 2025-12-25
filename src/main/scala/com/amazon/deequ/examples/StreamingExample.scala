/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.examples

import com.amazon.deequ.StreamingVerificationSuite
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
  * This example demonstrates how to use Deequ for continuous data quality monitoring
  * on streaming data using Spark Structured Streaming.
  *
  * The example shows:
  * 1. Setting up a streaming source (using rate source for demonstration)
  * 2. Defining data quality checks
  * 3. Running continuous quality monitoring
  * 4. Handling check results and failures
  */
private[examples] object StreamingExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Deequ Streaming Example")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/deequ-streaming-checkpoint")
    .getOrCreate()

  import spark.implicits._

  println("=" * 80)
  println("Deequ Streaming Data Quality Monitoring Example")
  println("=" * 80)

  // Create a streaming DataFrame - using rate source for demonstration
  // In production, you would use Kafka, Kinesis, or other streaming sources
  val streamingData = spark.readStream
    .format("rate")
    .option("rowsPerSecond", "10")
    .option("numPartitions", "2")
    .load()
    .selectExpr(
      "value as id",
      "cast(value % 100 as int) as product_id",
      "cast(rand() * 1000 as double) as amount",
      "timestamp",
      "case when value % 10 = 0 then null else 'active' end as status"
    )

  println("\nStarting streaming quality checks...")
  println("Monitoring the following metrics:")
  println("  - Data completeness (id, product_id, status)")
  println("  - Non-negative values (amount)")
  println("  - Record count per batch")
  println()

  // Create an in-memory state provider to maintain metrics across batches
  val stateProvider = InMemoryStateProvider()

  // Define quality checks
  val qualityCheck = Check(CheckLevel.Error, "streaming data quality")
    .isComplete("id")                      // id should never be null
    .isComplete("product_id")              // product_id should never be null
    .hasCompleteness("status", _ >= 0.8)  // At least 80% of status values should be non-null
    .isNonNegative("amount")               // amount should not be negative

  val warningCheck = Check(CheckLevel.Warning, "streaming data warnings")
    .hasSize(_ > 0)  // Each batch should have at least one record

  // Set up streaming verification suite
  val streamingQuery = StreamingVerificationSuite()
    .onData(streamingData)
    .addCheck(qualityCheck)
    .addCheck(warningCheck)
    .useStateProvider(stateProvider)
    .withTrigger(Trigger.ProcessingTime("5 seconds"))
    .withQueryName("deequ-streaming-quality-monitor")
    .onCheckComplete { (verificationResult, batchId) =>
      println(s"\n[$batchId] Batch processed at ${java.time.Instant.now()}")
      println(s"  Status: ${verificationResult.status}")

      if (verificationResult.status == CheckStatus.Success) {
        println("  ✓ All quality checks passed!")
      } else {
        println("  ✗ Quality check failures detected:")
        verificationResult.checkResults.foreach { case (check, checkResult) =>
          checkResult.constraintResults
            .filter(_.status != com.amazon.deequ.constraints.ConstraintStatus.Success)
            .foreach { result =>
              println(s"    - ${result.constraint}: ${result.message.getOrElse("failed")}")
            }
        }
      }

      // Display computed metrics
      if (verificationResult.metrics.nonEmpty) {
        println("  Metrics computed:")
        verificationResult.metrics.take(5).foreach { case (analyzer, metric) =>
          println(s"    - ${analyzer.toString.take(50)}: ${metric.value}")
        }
      }
    }
    .onCheckFailure { (verificationResult, batchId) =>
      // This callback is invoked only when checks fail
      // In production, you might want to:
      // - Send alerts (email, Slack, PagerDuty)
      // - Log to monitoring systems
      // - Trigger remediation workflows
      println(s"\n⚠ ALERT: Quality checks failed in batch $batchId")
    }
    .start()

  println(s"Streaming query started: ${streamingQuery.name}")
  println(s"Query ID: ${streamingQuery.id}")
  println("\nPress Ctrl+C to stop the query...")
  println("=" * 80)

  // Wait for the streaming query to terminate (or timeout after 60 seconds for demo)
  try {
    streamingQuery.awaitTermination(60000) // Run for 60 seconds in demo mode
  } catch {
    case _: Exception =>
      println("\nStopping streaming query...")
  } finally {
    streamingQuery.stop()
    println("\nStreaming query stopped.")
    println("=" * 80)
    spark.stop()
  }
}
