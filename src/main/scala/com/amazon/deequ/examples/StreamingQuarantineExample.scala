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
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.quarantine.{QuarantineConfig, QuarantineEngine, RowLevelConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * This example demonstrates Databricks DQX-style quarantine functionality for streaming data.
  *
  * Key DQX-inspired features:
  * 1. Split data into valid and invalid (quarantined) streams
  * 2. Row-level quality marking
  * 3. Separate sinks for clean and quarantined data
  * 4. Real-time quality monitoring with alerting
  */
private[examples] object StreamingQuarantineExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Deequ Streaming Quarantine Example (DQX-style)")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/deequ-quarantine-checkpoint")
    .getOrCreate()

  import spark.implicits._

  println("=" * 80)
  println("Deequ Streaming with Quarantine (DQX-style)")
  println("=" * 80)

  // Create a streaming source with some invalid data
  val streamingData = spark.readStream
    .format("rate")
    .option("rowsPerSecond", "10")
    .load()
    .selectExpr(
      "value as id",
      "cast(value % 100 as int) as customer_id",
      "cast(rand() * 1000 as double) as amount",
      "timestamp",
      "case when value % 10 = 0 then null else 'active' end as status",
      "case when value % 20 = 0 then null else concat('user', cast(value as string), '@example.com') end as email"
    )

  println("\nPattern: Split streaming data into valid and quarantine sinks")
  println("This mirrors Databricks DQX pattern for real-time data quality")
  println()

  // Define quality checks
  val qualityChecks = Check(CheckLevel.Error, "critical data quality")
    .isComplete("id")           // ID must never be null
    .isComplete("customer_id")  // Customer ID must never be null
    .isComplete("status")       // Status must not be null
    .isNonNegative("amount")   // Amount must be non-negative

  val warningChecks = Check(CheckLevel.Warning, "data warnings")
    .hasCompleteness("email", _ >= 0.95)  // At least 95% should have email

  // Create state provider for maintaining metrics across batches
  val stateProvider = InMemoryStateProvider()

  println("Starting streaming with quarantine pattern...")
  println("=" * 80)

  // Method 1: Using foreachBatch with QuarantineEngine (DQX-style)
  val quarantineQuery = streamingData.writeStream
    .foreachBatch { (batchDF, batchId) =>
      println(s"\n[Batch $batchId] Processing at ${java.time.Instant.now()}")

      // Apply checks with quarantine using DQX-style engine
      val result = QuarantineEngine.runWithQuarantine(
        data = batchDF,
        checks = Seq(qualityChecks, warningChecks),
        aggregateWith = Some(stateProvider),
        saveStatesWith = Some(stateProvider),
        rowLevelConfig = RowLevelConfig(
          markInvalidRows = true,
          invalidMarkerColumn = "_is_quarantined",
          checkFailureColumns = true,
          dropCheckColumns = false
        ),
        quarantineConfig = QuarantineConfig(
          quarantineOnError = true,
          quarantineOnWarning = false,
          addMetadataColumns = true,
          metadataPrefix = "_deequ_"
        )
      )

      // Display results
      println(s"  Valid records: ${result.validData.count()}")
      println(s"  Quarantined records: ${result.quarantinedData.count()}")
      println(s"  Check status: ${result.verificationResult.status}")

      if (result.verificationResult.status != com.amazon.deequ.checks.CheckStatus.Success) {
        println("  ⚠ Quality issues detected:")
        result.verificationResult.checkResults.foreach { case (check, checkResult) =>
          checkResult.constraintResults
            .filter(_.status != com.amazon.deequ.constraints.ConstraintStatus.Success)
            .foreach { constraint =>
              println(s"    - ${constraint.constraint}: ${constraint.message.getOrElse("failed")}")
            }
        }
      }

      // In production, write to different sinks:
      // result.validData.write.format("delta").mode("append").saveAsTable("prod.clean_data")
      // result.quarantinedData.write.format("delta").mode("append").saveAsTable("prod.quarantine")

      // For demo, just show a sample
      if (result.quarantinedData.count() > 0) {
        println("\n  Sample of quarantined records:")
        result.quarantinedData.select("id", "status", "email", "_is_quarantined", "_deequ_checks")
          .show(5, truncate = false)
      }
    }
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "/tmp/deequ-quarantine-checkpoint/main")
    .start()

  println("\nStreaming query started with quarantine pattern")
  println("Valid data would go to production table")
  println("Quarantined data would go to review/quarantine table")
  println("\nPress Ctrl+C to stop...")
  println("=" * 80)

  // Run for 60 seconds
  try {
    quarantineQuery.awaitTermination(60000)
  } catch {
    case _: Exception =>
      println("\nStopping quarantine query...")
  } finally {
    quarantineQuery.stop()
    println("\nQuarantine query stopped.")

    // Show cumulative statistics
    println("\n" + "=" * 80)
    println("Session Summary:")
    println("=" * 80)
    println("This example demonstrated DQX-style quarantine patterns:")
    println("  ✓ Split data into valid and quarantined streams")
    println("  ✓ Row-level quality marking with metadata")
    println("  ✓ Real-time quality monitoring")
    println("  ✓ Separate sinks for clean and quarantined data")
    println("\nIn production, use this pattern to:")
    println("  - Write valid data to production tables")
    println("  - Route quarantined data for review")
    println("  - Alert on quality issues")
    println("  - Track quality trends over time")
    println("=" * 80)

    spark.stop()
  }
}
