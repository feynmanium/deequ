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

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.StreamingAnalysisRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.ResultKey
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * This example demonstrates how to use the StreamingAnalysisRunner for
  * continuous metrics computation on streaming data.
  *
  * StreamingAnalysisRunner is the lower-level API that gives you more control
  * over metrics computation without the constraint evaluation layer.
  */
private[examples] object StreamingAnalysisExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Deequ Streaming Analysis Example")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/deequ-streaming-analysis-checkpoint")
    .getOrCreate()

  import spark.implicits._

  println("=" * 80)
  println("Deequ Streaming Analysis Runner Example")
  println("=" * 80)

  // Create a streaming source - simulating e-commerce transactions
  val streamingTransactions = spark.readStream
    .format("rate")
    .option("rowsPerSecond", "20")
    .load()
    .selectExpr(
      "value as transaction_id",
      "cast(value % 1000 as int) as user_id",
      "cast(rand() * 500 + 10 as double) as amount",
      "timestamp",
      "case when rand() > 0.95 then null else 'US' end as country",
      "case when rand() > 0.90 then 'fraud' else 'valid' end as status"
    )

  println("\nComputing streaming metrics:")
  println("  - Transaction count")
  println("  - Completeness of country field")
  println("  - Mean transaction amount")
  println("  - Distinct users")
  println("  - Compliance rate (valid transactions)")
  println()

  // Create state provider and metrics repository
  val stateProvider = InMemoryStateProvider()
  val metricsRepository = new InMemoryMetricsRepository()

  // Define analyzers
  val analyzers = Seq(
    Size(),                                            // Total transaction count
    Completeness("country"),                          // % of non-null countries
    Mean("amount"),                                   // Average transaction amount
    ApproxCountDistinct("user_id"),                  // Approximate distinct users
    Compliance("valid_transactions", "status = 'valid'")  // % of valid transactions
  )

  // Start streaming analysis
  val streamingQuery = StreamingAnalysisRunner
    .onData(streamingTransactions)
    .addAnalyzers(analyzers)
    .useStateProvider(stateProvider)
    .withTrigger(Trigger.ProcessingTime("5 seconds"))
    .withQueryName("streaming-metrics-analysis")
    .saveMetricsTo(
      metricsRepository,
      batchId => ResultKey(batchId, Map("dataset" -> "transactions"))
    )
    .onBatchComplete { (analyzerContext, batchId) =>
      println(s"\n[Batch $batchId] Metrics computed:")
      println(s"  Timestamp: ${java.time.Instant.now()}")

      // Display all metrics
      analyzerContext.allMetrics.foreach { case (analyzer, metric) =>
        val analyzerName = analyzer match {
          case Size(_) => "Transaction Count"
          case Completeness(column, _) => s"Completeness($column)"
          case Mean(column, _) => s"Mean($column)"
          case ApproxCountDistinct(columns, _) => s"Distinct Users"
          case Compliance(name, _, _, _) => s"Compliance($name)"
          case _ => analyzer.toString.take(40)
        }

        metric.value match {
          case scala.util.Success(value: Double) =>
            val formattedValue = analyzer match {
              case Size(_) => f"$value%.0f"
              case Completeness(_, _) => f"${value * 100}%.1f%%"
              case Mean(_, _) => f"$$$$value%.2f"
              case ApproxCountDistinct(_, _) => f"$value%.0f"
              case Compliance(_, _, _, _) => f"${value * 100}%.1f%%"
              case _ => f"$value%.3f"
            }
            println(s"    ✓ $analyzerName: $formattedValue")
          case scala.util.Failure(exception) =>
            println(s"    ✗ $analyzerName: Failed - ${exception.getMessage}")
        }
      }

      // Demonstrate accessing specific metrics
      val sizeMetric = analyzerContext.metric(Size())
      sizeMetric.foreach { metric =>
        metric.value.foreach { count =>
          if (count > 100) {
            println(s"\n  📊 Running totals (aggregated across all batches):")
            println(s"    Total transactions processed: ${count.toLong}")
          }
        }
      }
    }
    .start()

  println(s"\nStreaming query started: ${streamingQuery.name}")
  println(s"Query ID: ${streamingQuery.id}")
  println("\nMonitoring metrics for 60 seconds...")
  println("=" * 80)

  // Run for 60 seconds then stop
  try {
    streamingQuery.awaitTermination(60000)
  } catch {
    case _: Exception =>
      println("\nStopping streaming query...")
  } finally {
    streamingQuery.stop()
    println("\n" + "=" * 80)
    println("Streaming query stopped.")

    // Display summary of all stored metrics
    println("\nMetrics Repository Summary:")
    println(s"  Total batches processed: ${metricsRepository.load().get().size}")

    println("=" * 80)
    spark.stop()
  }
}
