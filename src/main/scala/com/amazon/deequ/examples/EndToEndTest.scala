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

import com.amazon.deequ.{StreamingVerificationSuite, VerificationSuite}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, StreamingAnalysisRunner}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.quarantine.{QuarantineConfig, QuarantineEngine, RowLevelConfig}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.ResultKey
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

/**
  * Comprehensive end-to-end test for all streaming and DQX functionality.
  * This is a runnable example that tests all features.
  */
object EndToEndTest extends App {

  case class TestData(
      id: Int,
      name: String,
      amount: Double,
      status: String,
      email: String)

  val spark = SparkSession
    .builder()
    .appName("Deequ End-to-End Test")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/deequ-e2e-test")
    .getOrCreate()

  import spark.implicits._

  var testsPassed = 0
  var testsFailed = 0

  def runTest(testName: String)(testBody: => Unit): Unit = {
    println(s"\n${"=" * 80}")
    println(s"TEST: $testName")
    println("=" * 80)
    try {
      testBody
      testsPassed += 1
      println(s"✓ PASSED: $testName")
    } catch {
      case e: Exception =>
        testsFailed += 1
        println(s"✗ FAILED: $testName")
        println(s"  Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  println("\n" + "=" * 80)
  println("DEEQU STREAMING & DQX END-TO-END TEST SUITE")
  println("=" * 80)

  // ==========================================================================
  // Test 1: Basic Batch Quarantine (Non-Streaming)
  // ==========================================================================
  runTest("Batch Quarantine - Split Valid and Invalid Data") {
    val batchData = Seq(
      TestData(1, "Alice", 100.0, "active", "alice@example.com"),
      TestData(2, "Bob", 200.0, "active", "bob@example.com"),
      TestData(3, null, 300.0, "active", "charlie@example.com"),  // Invalid: null name
      TestData(4, "Dave", -50.0, "active", "dave@example.com"),    // Invalid: negative amount
      TestData(5, "Eve", 500.0, "active", "eve@example.com")
    ).toDF()

    val check = Check(CheckLevel.Error, "quality check")
      .isComplete("name")
      .isNonNegative("amount")

    val result = QuarantineEngine.runWithQuarantine(
      data = batchData,
      checks = Seq(check),
      quarantineConfig = QuarantineConfig(quarantineOnError = true)
    )

    val validCount = result.validData.count()
    val quarantinedCount = result.quarantinedData.count()

    println(s"  Valid records: $validCount")
    println(s"  Quarantined records: $quarantinedCount")

    assert(validCount == 3, s"Expected 3 valid records, got $validCount")
    assert(quarantinedCount == 2, s"Expected 2 quarantined records, got $quarantinedCount")
    assert(result.verificationResult.status == CheckStatus.Error, "Expected Error status")
  }

  // ==========================================================================
  // Test 2: Batch getValid and getInvalid
  // ==========================================================================
  runTest("Batch getValid/getInvalid Filters") {
    val batchData = Seq(
      TestData(1, "Alice", 100.0, "active", "alice@example.com"),
      TestData(2, "Bob", 200.0, "active", "bob@example.com"),
      TestData(3, null, 300.0, "active", null)  // Multiple issues
    ).toDF()

    val check = Check(CheckLevel.Error, "check")
      .isComplete("name")
      .isComplete("email")

    val result = QuarantineEngine.runWithQuarantine(
      data = batchData,
      checks = Seq(check),
      rowLevelConfig = RowLevelConfig(markInvalidRows = true, dropCheckColumns = false)
    )

    // Use getValid and getInvalid on the combined data
    val allData = result.validData.union(result.quarantinedData)
    val validFiltered = QuarantineEngine.getValid(allData, dropCheckColumns = false)
    val invalidFiltered = QuarantineEngine.getInvalid(allData)

    println(s"  Valid filtered: ${validFiltered.count()}")
    println(s"  Invalid filtered: ${invalidFiltered.count()}")

    assert(validFiltered.count() == 2, "Expected 2 valid records from filter")
    assert(invalidFiltered.count() == 1, "Expected 1 invalid record from filter")
  }

  // ==========================================================================
  // Test 3: Streaming Analysis Runner
  // ==========================================================================
  runTest("Streaming Analysis Runner - Compute Metrics") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    val stateProvider = InMemoryStateProvider()
    var metricsCollected = scala.collection.mutable.ArrayBuffer[AnalyzerContext]()

    val analyzers = Seq(
      Size(),
      Completeness("name"),
      Mean("amount")
    )

    val query = StreamingAnalysisRunner
      .onData(streamingData)
      .addAnalyzers(analyzers)
      .useStateProvider(stateProvider)
      .withTrigger(Trigger.Once())
      .onBatchComplete { (context, batchId) =>
        metricsCollected.append(context)
      }
      .start()

    try {
      // Batch 1
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com"),
        TestData(2, "Bob", 200.0, "active", "bob@example.com")
      )
      query.processAllAvailable()

      // Batch 2
      memoryStream.addData(
        TestData(3, "Charlie", 300.0, "active", "charlie@example.com")
      )
      query.processAllAvailable()

      assert(metricsCollected.size >= 1, "Expected at least 1 batch processed")

      val lastContext = metricsCollected.last
      val sizeMetric = lastContext.metric(Size())

      assert(sizeMetric.isDefined, "Size metric should be defined")
      println(s"  Total records processed: ${sizeMetric.get.value.get}")

      // Should be 3 (cumulative across batches)
      assert(sizeMetric.get.value.get == 3.0, s"Expected 3 total records, got ${sizeMetric.get.value.get}")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Test 4: Streaming Verification Suite
  // ==========================================================================
  runTest("Streaming Verification Suite - Run Checks") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    var checkResults = scala.collection.mutable.ArrayBuffer[CheckStatus.Value]()

    val check = Check(CheckLevel.Error, "streaming check")
      .isComplete("id")
      .isComplete("name")
      .hasCompleteness("email", _ >= 0.8)

    val query = StreamingVerificationSuite()
      .onData(streamingData)
      .addCheck(check)
      .withTrigger(Trigger.Once())
      .onCheckComplete { (result, batchId) =>
        checkResults.append(result.status)
      }
      .start()

    try {
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com"),
        TestData(2, "Bob", 200.0, "active", "bob@example.com"),
        TestData(3, "Charlie", 300.0, "active", "charlie@example.com")
      )
      query.processAllAvailable()

      assert(checkResults.nonEmpty, "Expected check results")
      println(s"  Check status: ${checkResults.head}")
      assert(checkResults.head == CheckStatus.Success, "Expected checks to pass")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Test 5: Streaming Verification with Failures
  // ==========================================================================
  runTest("Streaming Verification Suite - Detect Failures") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    var failureDetected = false

    val check = Check(CheckLevel.Error, "strict check")
      .hasCompleteness("name", _ == 1.0)  // All names must be non-null

    val query = StreamingVerificationSuite()
      .onData(streamingData)
      .addCheck(check)
      .withTrigger(Trigger.Once())
      .onCheckFailure { (result, batchId) =>
        failureDetected = true
      }
      .start()

    try {
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com"),
        TestData(2, null, 200.0, "active", "bob@example.com")  // Null name
      )
      query.processAllAvailable()

      assert(failureDetected, "Expected failure to be detected")
      println("  ✓ Failure callback was triggered")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Test 6: Streaming with State Aggregation
  // ==========================================================================
  runTest("Streaming with State Aggregation Across Batches") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    val stateProvider = InMemoryStateProvider()
    val counts = scala.collection.mutable.ArrayBuffer[Double]()

    val query = StreamingAnalysisRunner
      .onData(streamingData)
      .addAnalyzer(Size())
      .useStateProvider(stateProvider)
      .withTrigger(Trigger.Once())
      .onBatchComplete { (context, batchId) =>
        context.metric(Size()).foreach { metric =>
          metric.value.foreach(counts.append(_))
        }
      }
      .start()

    try {
      // Batch 1: 2 records
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com"),
        TestData(2, "Bob", 200.0, "active", "bob@example.com")
      )
      query.processAllAvailable()

      // Batch 2: 3 more records
      memoryStream.addData(
        TestData(3, "Charlie", 300.0, "active", "charlie@example.com"),
        TestData(4, "Dave", 400.0, "active", "dave@example.com"),
        TestData(5, "Eve", 500.0, "active", "eve@example.com")
      )
      query.processAllAvailable()

      assert(counts.size == 2, s"Expected 2 batches, got ${counts.size}")
      println(s"  Batch 1 count: ${counts(0)}")
      println(s"  Batch 2 count: ${counts(1)}")

      assert(counts(0) == 2.0, s"Expected 2 in batch 1, got ${counts(0)}")
      assert(counts(1) == 5.0, s"Expected 5 cumulative in batch 2, got ${counts(1)}")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Test 7: Metrics Repository Integration
  // ==========================================================================
  runTest("Metrics Repository Integration") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    val metricsRepository = new InMemoryMetricsRepository()

    val query = StreamingAnalysisRunner
      .onData(streamingData)
      .addAnalyzer(Size())
      .addAnalyzer(Completeness("name"))
      .saveMetricsTo(
        metricsRepository,
        batchId => ResultKey(batchId, Map("test" -> "e2e"))
      )
      .withTrigger(Trigger.Once())
      .start()

    try {
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com")
      )
      query.processAllAvailable()

      val savedMetrics = metricsRepository.load().get()
      assert(savedMetrics.nonEmpty, "Expected metrics in repository")
      println(s"  Metrics saved: ${savedMetrics.size} entries")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Test 8: Quarantine with Metadata Columns
  // ==========================================================================
  runTest("Quarantine with Metadata Columns") {
    val batchData = Seq(
      TestData(1, "Alice", 100.0, "active", "alice@example.com"),
      TestData(2, null, 200.0, "active", "bob@example.com")
    ).toDF()

    val check = Check(CheckLevel.Error, "metadata test")
      .isComplete("name")

    val result = QuarantineEngine.runWithQuarantine(
      data = batchData,
      checks = Seq(check),
      rowLevelConfig = RowLevelConfig(markInvalidRows = true),
      quarantineConfig = QuarantineConfig(
        quarantineOnError = true,
        addMetadataColumns = true,
        metadataPrefix = "_deequ_"
      )
    )

    val quarantinedColumns = result.quarantinedData.columns
    println(s"  Quarantined columns: ${quarantinedColumns.mkString(", ")}")

    assert(quarantinedColumns.contains("_deequ_checked_at"), "Expected timestamp column")
    assert(quarantinedColumns.contains("_deequ_checks"), "Expected checks column")

  }

  // ==========================================================================
  // Test 9: Multiple Analyzers
  // ==========================================================================
  runTest("Multiple Analyzers - Size, Completeness, Mean") {
    val batchData = Seq(
      TestData(1, "Alice", 100.0, "active", "alice@example.com"),
      TestData(2, "Bob", 200.0, "active", "bob@example.com"),
      TestData(3, "Charlie", 300.0, "active", "charlie@example.com")
    ).toDF()

    val stateProvider = InMemoryStateProvider()

    val context = com.amazon.deequ.analyzers.runners.AnalysisRunner.run(
      data = batchData,
      analysis = Analysis(Seq(
        Size(),
        Completeness("name"),
        Completeness("email"),
        Mean("amount")
      )),
      saveStatesWith = Some(stateProvider)
    )

    val sizeMetric = context.metric(Size())
    val nameCompleteness = context.metric(Completeness("name"))
    val meanAmount = context.metric(Mean("amount"))

    assert(sizeMetric.isDefined && sizeMetric.get.value.get == 3.0, "Size should be 3")
    assert(nameCompleteness.isDefined && nameCompleteness.get.value.get == 1.0, "Name completeness should be 1.0")
    assert(meanAmount.isDefined && meanAmount.get.value.get == 200.0, "Mean amount should be 200.0")

    println(s"  Size: ${sizeMetric.get.value.get}")
    println(s"  Name Completeness: ${nameCompleteness.get.value.get}")
    println(s"  Mean Amount: ${meanAmount.get.value.get}")
  }

  // ==========================================================================
  // Test 10: End-to-End Streaming with Quarantine
  // ==========================================================================
  runTest("End-to-End Streaming with Quarantine Pattern") {
    implicit val ctx = spark.sqlContext
    val memoryStream = MemoryStream[TestData]

    val streamingData = memoryStream.toDF()
    val stateProvider = InMemoryStateProvider()
    var totalValid = 0L
    var totalQuarantined = 0L

    val check = Check(CheckLevel.Error, "e2e check")
      .isComplete("name")
      .isNonNegative("amount")

    val query = streamingData.writeStream
      .foreachBatch { (batchDF, batchId) =>
        val result = QuarantineEngine.runWithQuarantine(
          data = batchDF,
          checks = Seq(check),
          aggregateWith = Some(stateProvider),
          saveStatesWith = Some(stateProvider),
          quarantineConfig = QuarantineConfig(quarantineOnError = true)
        )

        totalValid += result.validData.count()
        totalQuarantined += result.quarantinedData.count()

        println(s"  Batch $batchId: valid=${result.validData.count()}, quarantined=${result.quarantinedData.count()}")
      }
      .trigger(Trigger.Once())
      .start()

    try {
      // Batch 1: Mix of valid and invalid
      memoryStream.addData(
        TestData(1, "Alice", 100.0, "active", "alice@example.com"),
        TestData(2, null, 200.0, "active", "bob@example.com"),  // Invalid
        TestData(3, "Charlie", 300.0, "active", "charlie@example.com")
      )
      query.processAllAvailable()

      // Batch 2: More data
      memoryStream.addData(
        TestData(4, "Dave", -50.0, "active", "dave@example.com"),  // Invalid
        TestData(5, "Eve", 500.0, "active", "eve@example.com")
      )
      query.processAllAvailable()

      println(s"  Total valid: $totalValid")
      println(s"  Total quarantined: $totalQuarantined")

      assert(totalValid == 3, s"Expected 3 total valid, got $totalValid")
      assert(totalQuarantined == 2, s"Expected 2 total quarantined, got $totalQuarantined")

    } finally {
      query.stop()
    }
  }

  // ==========================================================================
  // Print Summary
  // ==========================================================================
  println("\n" + "=" * 80)
  println("TEST SUMMARY")
  println("=" * 80)
  println(s"Tests Passed: $testsPassed")
  println(s"Tests Failed: $testsFailed")
  println(s"Total Tests:  ${testsPassed + testsFailed}")
  println("=" * 80)

  if (testsFailed == 0) {
    println("✓ ALL TESTS PASSED!")
    println("=" * 80)
    spark.stop()
    System.exit(0)
  } else {
    println("✗ SOME TESTS FAILED")
    println("=" * 80)
    spark.stop()
    System.exit(1)
  }
}
