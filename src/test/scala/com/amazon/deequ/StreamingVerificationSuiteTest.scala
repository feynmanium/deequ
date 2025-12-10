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

package com.amazon.deequ

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, StreamingAnalysisRunner}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.ResultKey
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class StreamingVerificationSuiteTest extends WordSpec with Matchers with SparkContextSpec {

  case class TestRecord(id: Int, name: String, value: Double, status: String)

  "StreamingVerificationSuite" should {

    "process streaming data and run checks on each batch" in withSparkSession { spark =>
      import spark.implicits._

      // Create memory stream for testing
      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      // Track check results
      val checkResults = ArrayBuffer[(CheckStatus.Value, Long)]()

      // Create checks
      val check = Check(CheckLevel.Error, "test check")
        .isComplete("id")
        .isComplete("name")
        .hasCompleteness("status", _ >= 0.8)

      // Start streaming verification
      val query = StreamingVerificationSuite()
        .onData(streamingData)
        .addCheck(check)
        .withTrigger(Trigger.Once())
        .onCheckComplete { (result, batchId) =>
          checkResults.append((result.status, batchId))
        }
        .start()

      try {
        // Add test data
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active"),
          TestRecord(2, "Bob", 200.0, "active"),
          TestRecord(3, "Charlie", 300.0, "active")
        )

        query.processAllAvailable()

        // Verify check was executed
        assert(checkResults.nonEmpty)
        assert(checkResults.head._1 == CheckStatus.Success)

      } finally {
        query.stop()
      }
    }

    "detect check failures in streaming data" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val failureResults = ArrayBuffer[(VerificationResult, Long)]()

      // Create a check that will fail
      val check = Check(CheckLevel.Error, "completeness check")
        .hasCompleteness("name", _ == 1.0) // Will fail with null names

      val query = StreamingVerificationSuite()
        .onData(streamingData)
        .addCheck(check)
        .withTrigger(Trigger.Once())
        .onCheckFailure { (result, batchId) =>
          failureResults.append((result, batchId))
        }
        .start()

      try {
        // Add data with null names
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active"),
          TestRecord(2, null, 200.0, "active"), // null name
          TestRecord(3, "Charlie", 300.0, "active")
        )

        query.processAllAvailable()

        // Verify failure was detected
        assert(failureResults.nonEmpty)
        assert(failureResults.head._1.status == CheckStatus.Error)

      } finally {
        query.stop()
      }
    }

    "maintain state across multiple batches" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val stateProvider = InMemoryStateProvider()
      val batchCounts = ArrayBuffer[Double]()

      val query = StreamingVerificationSuite()
        .onData(streamingData)
        .addCheck(Check(CheckLevel.Error, "test").hasSize(_ > 0))
        .useStateProvider(stateProvider)
        .withTrigger(Trigger.Once())
        .onCheckComplete { (result, batchId) =>
          // Extract size metric
          result.metrics.get(Size()).foreach { metric =>
            metric.value.foreach { v =>
              batchCounts.append(v)
            }
          }
        }
        .start()

      try {
        // First batch
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active"),
          TestRecord(2, "Bob", 200.0, "active")
        )
        query.processAllAvailable()

        // Second batch
        memoryStream.addData(
          TestRecord(3, "Charlie", 300.0, "active"),
          TestRecord(4, "Dave", 400.0, "active"),
          TestRecord(5, "Eve", 500.0, "active")
        )
        query.processAllAvailable()

        // Verify counts accumulate across batches
        assert(batchCounts.size == 2)
        assert(batchCounts(0) == 2.0) // First batch: 2 records
        assert(batchCounts(1) == 5.0) // Cumulative: 2 + 3 = 5 records

      } finally {
        query.stop()
      }
    }

    "save metrics to repository" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val metricsRepository = new InMemoryMetricsRepository()

      val query = StreamingVerificationSuite()
        .onData(streamingData)
        .addCheck(Check(CheckLevel.Error, "test")
          .isComplete("id")
          .hasSize(_ > 0))
        .saveMetricsTo(
          metricsRepository,
          batchId => ResultKey(batchId, Map("batch" -> batchId.toString))
        )
        .withTrigger(Trigger.Once())
        .start()

      try {
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active"),
          TestRecord(2, "Bob", 200.0, "active")
        )

        query.processAllAvailable()

        // Verify metrics were saved
        val savedMetrics = metricsRepository.load().get()
        assert(savedMetrics.nonEmpty)

      } finally {
        query.stop()
      }
    }

    "require streaming DataFrame" in withSparkSession { spark =>
      import spark.implicits._

      // Create a batch (non-streaming) DataFrame
      val batchData = Seq(
        TestRecord(1, "Alice", 100.0, "active")
      ).toDF()

      // Should throw an exception
      intercept[IllegalArgumentException] {
        StreamingVerificationSuite()
          .onData(batchData)
          .addCheck(Check(CheckLevel.Error, "test").hasSize(_ > 0))
          .start()
      }
    }

    "require at least one check" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      // Should throw an exception when no checks added
      intercept[IllegalArgumentException] {
        StreamingVerificationSuite()
          .onData(streamingData)
          .start()
      }
    }
  }

  "StreamingAnalysisRunner" should {

    "compute metrics on streaming data" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val metricsCollected = ArrayBuffer[AnalyzerContext]()

      val analyzers = Seq(
        Size(),
        Completeness("name"),
        Mean("value")
      )

      val query = StreamingAnalysisRunner
        .onData(streamingData)
        .addAnalyzers(analyzers)
        .withTrigger(Trigger.Once())
        .onBatchComplete { (context, batchId) =>
          metricsCollected.append(context)
        }
        .start()

      try {
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active"),
          TestRecord(2, "Bob", 200.0, "active"),
          TestRecord(3, "Charlie", 300.0, "active")
        )

        query.processAllAvailable()

        // Verify metrics were computed
        assert(metricsCollected.nonEmpty)
        val context = metricsCollected.head

        // Check Size metric
        val sizeMetric = context.metric(Size())
        assert(sizeMetric.isDefined)
        assert(sizeMetric.get.value.get == 3.0)

        // Check Completeness metric
        val completenessMetric = context.metric(Completeness("name"))
        assert(completenessMetric.isDefined)
        assert(completenessMetric.get.value.get == 1.0)

        // Check Mean metric
        val meanMetric = context.metric(Mean("value"))
        assert(meanMetric.isDefined)
        assert(meanMetric.get.value.get == 200.0)

      } finally {
        query.stop()
      }
    }

    "aggregate state across batches" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val stateProvider = InMemoryStateProvider()
      val sizesCollected = ArrayBuffer[Double]()

      val query = StreamingAnalysisRunner
        .onData(streamingData)
        .addAnalyzer(Size())
        .useStateProvider(stateProvider)
        .withTrigger(Trigger.Once())
        .onBatchComplete { (context, batchId) =>
          context.metric(Size()).foreach { metric =>
            metric.value.foreach(v => sizesCollected.append(v))
          }
        }
        .start()

      try {
        // Batch 1
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active")
        )
        query.processAllAvailable()

        // Batch 2
        memoryStream.addData(
          TestRecord(2, "Bob", 200.0, "active"),
          TestRecord(3, "Charlie", 300.0, "active")
        )
        query.processAllAvailable()

        // Batch 3
        memoryStream.addData(
          TestRecord(4, "Dave", 400.0, "active")
        )
        query.processAllAvailable()

        // Verify cumulative counts
        assert(sizesCollected.size == 3)
        assert(sizesCollected(0) == 1.0) // First batch
        assert(sizesCollected(1) == 3.0) // First + Second = 1 + 2 = 3
        assert(sizesCollected(2) == 4.0) // All batches = 1 + 2 + 1 = 4

      } finally {
        query.stop()
      }
    }

    "save metrics to repository" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      val metricsRepository = new InMemoryMetricsRepository()

      val query = StreamingAnalysisRunner
        .onData(streamingData)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("name"))
        .saveMetricsTo(
          metricsRepository,
          batchId => ResultKey(batchId, Map("test" -> "streaming"))
        )
        .withTrigger(Trigger.Once())
        .start()

      try {
        memoryStream.addData(
          TestRecord(1, "Alice", 100.0, "active")
        )

        query.processAllAvailable()

        // Verify metrics were saved to repository
        val loadedMetrics = metricsRepository.load().get()
        assert(loadedMetrics.nonEmpty)
        assert(loadedMetrics.head._1.tags("test") == "streaming")

      } finally {
        query.stop()
      }
    }

    "require streaming DataFrame" in withSparkSession { spark =>
      import spark.implicits._

      val batchData = Seq(
        TestRecord(1, "Alice", 100.0, "active")
      ).toDF()

      intercept[IllegalArgumentException] {
        StreamingAnalysisRunner
          .onData(batchData)
          .addAnalyzer(Size())
          .start()
      }
    }

    "require at least one analyzer" in withSparkSession { spark =>
      import spark.implicits._

      implicit val ctx = spark.sqlContext
      val memoryStream = MemoryStream[TestRecord]

      val streamingData = memoryStream.toDF()

      intercept[IllegalArgumentException] {
        StreamingAnalysisRunner
          .onData(streamingData)
          .start()
      }
    }
  }
}
