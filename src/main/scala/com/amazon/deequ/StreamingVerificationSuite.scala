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
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, StreamingAnalysisOptions, StreamingAnalysisRunner}
import com.amazon.deequ.checks.{Check, CheckStatus}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.storage.StorageLevel

/**
  * Options for configuring streaming verification behavior
  */
private[deequ] case class StreamingVerificationOptions(
    stateProvider: Option[StateProvider] = None,
    metricsRepository: Option[MetricsRepository] = None,
    resultKeyGenerator: Option[Long => ResultKey] = None,
    trigger: Trigger = Trigger.ProcessingTime("1 minute"),
    checkpointLocation: Option[String] = None,
    queryName: Option[String] = None,
    onCheckComplete: Option[(VerificationResult, Long) => Unit] = None,
    onCheckFailure: Option[(VerificationResult, Long) => Unit] = None,
    storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)

/**
  * Responsible for running checks on streaming data and returning results for each micro-batch.
  * This enables continuous data quality monitoring on streaming datasets.
  *
  * Example usage:
  * {{{
  * val query = StreamingVerificationSuite()
  *   .onData(streamingDF)
  *   .addCheck(Check(CheckLevel.Error, "data quality")
  *     .hasSize(_ > 0)
  *     .isComplete("user_id")
  *     .hasCompleteness("email", _ > 0.9))
  *   .withTrigger(Trigger.ProcessingTime("1 minute"))
  *   .withCheckpointLocation("/path/to/checkpoint")
  *   .start()
  *
  * query.awaitTermination()
  * }}}
  */
class StreamingVerificationSuite {

  /**
    * Starting point to construct a streaming verification run.
    *
    * @param data streaming DataFrame on which the checks should be verified
    */
  def onData(data: DataFrame): StreamingVerificationRunBuilder = {
    require(data.isStreaming, "Input DataFrame must be a streaming DataFrame")
    new StreamingVerificationRunBuilder(data)
  }
}

/**
  * Convenience functions for using the StreamingVerificationSuite
  */
object StreamingVerificationSuite {

  def apply(): StreamingVerificationSuite = {
    new StreamingVerificationSuite()
  }
}

/**
  * Builder for streaming verification runs with a fluent API
  */
class StreamingVerificationRunBuilder(streamingData: DataFrame) {

  private var checks: Seq[Check] = Seq.empty
  private var requiredAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty
  private var stateProvider: Option[StateProvider] = None
  private var metricsRepository: Option[MetricsRepository] = None
  private var resultKeyGenerator: Option[Long => ResultKey] = None
  private var trigger: Trigger = Trigger.ProcessingTime("1 minute")
  private var checkpointLocation: Option[String] = None
  private var queryName: Option[String] = None
  private var onCheckComplete: Option[(VerificationResult, Long) => Unit] = None
  private var onCheckFailure: Option[(VerificationResult, Long) => Unit] = None
  private var storageLevelOfGroupedDataForMultiplePasses: StorageLevel =
    StorageLevel.MEMORY_AND_DISK

  /**
    * Add a single check to be run on each micro-batch
    */
  def addCheck(check: Check): StreamingVerificationRunBuilder = {
    checks = checks :+ check
    this
  }

  /**
    * Add multiple checks to be run on each micro-batch
    */
  def addChecks(newChecks: Seq[Check]): StreamingVerificationRunBuilder = {
    checks = checks ++ newChecks
    this
  }

  /**
    * Add required analyzers that should be run regardless of checks
    */
  def addRequiredAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): StreamingVerificationRunBuilder = {
    requiredAnalyzers = requiredAnalyzers ++ analyzers
    this
  }

  /**
    * Add a required analyzer that should be run regardless of checks
    */
  def addRequiredAnalyzer(analyzer: Analyzer[_, Metric[_]]): StreamingVerificationRunBuilder = {
    requiredAnalyzers = requiredAnalyzers :+ analyzer
    this
  }

  /**
    * Set the state provider for maintaining state across batches
    */
  def useStateProvider(provider: StateProvider): StreamingVerificationRunBuilder = {
    stateProvider = Some(provider)
    this
  }

  /**
    * Save metrics to a repository after each batch
    */
  def saveMetricsTo(
      repository: MetricsRepository,
      keyGenerator: Long => ResultKey): StreamingVerificationRunBuilder = {
    metricsRepository = Some(repository)
    resultKeyGenerator = Some(keyGenerator)
    this
  }

  /**
    * Set the trigger for the streaming query
    */
  def withTrigger(newTrigger: Trigger): StreamingVerificationRunBuilder = {
    trigger = newTrigger
    this
  }

  /**
    * Set the checkpoint location for the streaming query
    */
  def withCheckpointLocation(location: String): StreamingVerificationRunBuilder = {
    checkpointLocation = Some(location)
    this
  }

  /**
    * Set the query name
    */
  def withQueryName(name: String): StreamingVerificationRunBuilder = {
    queryName = Some(name)
    this
  }

  /**
    * Register a callback to be invoked after each batch is processed successfully
    */
  def onCheckComplete(callback: (VerificationResult, Long) => Unit): StreamingVerificationRunBuilder = {
    onCheckComplete = Some(callback)
    this
  }

  /**
    * Register a callback to be invoked when checks fail in a batch
    */
  def onCheckFailure(callback: (VerificationResult, Long) => Unit): StreamingVerificationRunBuilder = {
    onCheckFailure = Some(callback)
    this
  }

  /**
    * Set the storage level for grouped data that must be accessed multiple times
    */
  def useStorageLevel(level: StorageLevel): StreamingVerificationRunBuilder = {
    storageLevelOfGroupedDataForMultiplePasses = level
    this
  }

  /**
    * Start the streaming verification query
    */
  def start(): StreamingQuery = {

    if (checks.isEmpty) {
      throw new IllegalArgumentException("At least one check must be added")
    }

    // Collect all required analyzers from checks
    val allAnalyzers = requiredAnalyzers ++ checks.flatMap(_.requiredAnalyzers())

    // Create callback that evaluates checks
    val batchCallback: (AnalyzerContext, Long) => Unit = (analyzerContext, batchId) => {
      // Evaluate checks against the metrics
      val verificationResult = evaluateChecks(checks, analyzerContext)

      // Invoke success callback
      onCheckComplete.foreach(_(verificationResult, batchId))

      // Invoke failure callback if checks failed
      if (verificationResult.status != CheckStatus.Success) {
        onCheckFailure.foreach(_(verificationResult, batchId))
      }
    }

    // Build streaming analysis options
    val streamingOptions = StreamingAnalysisOptions(
      stateProvider = stateProvider,
      metricsRepository = metricsRepository,
      resultKeyGenerator = resultKeyGenerator,
      trigger = trigger,
      checkpointLocation = checkpointLocation,
      queryName = queryName.orElse(Some("deequ-streaming-verification")),
      onBatchComplete = Some(batchCallback),
      storageLevelOfGroupedDataForMultiplePasses = storageLevelOfGroupedDataForMultiplePasses
    )

    // Start the streaming analysis
    StreamingAnalysisRunner.run(
      streamingData,
      Analysis(allAnalyzers),
      streamingOptions
    )
  }

  /**
    * Evaluate checks against the analyzer context
    */
  private def evaluateChecks(
      checks: Seq[Check],
      analysisContext: AnalyzerContext)
    : VerificationResult = {

    val checkResults = checks
      .map { check => check -> check.evaluate(analysisContext) }
      .toMap

    val verificationStatus = if (checkResults.isEmpty) {
      CheckStatus.Success
    } else {
      checkResults.values
        .map { _.status }
        .max
    }

    VerificationResult(verificationStatus, checkResults, analysisContext.metricMap)
  }
}
