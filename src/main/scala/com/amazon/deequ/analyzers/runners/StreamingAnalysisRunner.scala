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

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers._
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.storage.StorageLevel

/**
  * Options for configuring streaming analysis behavior
  */
private[deequ] case class StreamingAnalysisOptions(
    stateProvider: Option[StateProvider] = None,
    metricsRepository: Option[MetricsRepository] = None,
    resultKeyGenerator: Option[Long => ResultKey] = None,
    trigger: Trigger = Trigger.ProcessingTime("1 minute"),
    checkpointLocation: Option[String] = None,
    queryName: Option[String] = None,
    onBatchComplete: Option[(AnalyzerContext, Long) => Unit] = None,
    storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)

/**
  * Runs a set of analyzers on streaming data, processing each micro-batch and maintaining
  * state across batches. This enables continuous data quality monitoring on streaming datasets.
  *
  * The streaming analysis runner leverages Deequ's algebraic state architecture to efficiently
  * aggregate metrics across streaming micro-batches, providing real-time data quality insights.
  */
object StreamingAnalysisRunner {

  /**
    * Starting point to construct a streaming analysis run.
    *
    * @param streamingData streaming DataFrame to analyze
    */
  def onData(streamingData: DataFrame): StreamingAnalysisRunBuilder = {
    require(streamingData.isStreaming, "Input DataFrame must be a streaming DataFrame")
    new StreamingAnalysisRunBuilder(streamingData)
  }

  /**
    * Run analyzers on streaming data, processing each micro-batch and aggregating state.
    *
    * @param streamingData streaming DataFrame to analyze
    * @param analysis analysis defining the analyzers to run
    * @param options configuration options for streaming analysis
    * @return StreamingQuery that can be started and monitored
    */
  def run(
      streamingData: DataFrame,
      analysis: Analysis,
      options: StreamingAnalysisOptions = StreamingAnalysisOptions())
    : StreamingQuery = {

    require(streamingData.isStreaming, "Input DataFrame must be a streaming DataFrame")

    val analyzers = analysis.analyzers

    if (analyzers.isEmpty) {
      throw new IllegalArgumentException("Analysis must contain at least one analyzer")
    }

    // Create or use provided state provider
    val stateProvider = options.stateProvider.getOrElse(InMemoryStateProvider())

    // Process each micro-batch
    val query = streamingData.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        processBatch(
          batchDF,
          batchId,
          analyzers,
          stateProvider,
          options
        )
      }
      .trigger(options.trigger)

    // Set checkpoint location if provided
    val queryWithCheckpoint = options.checkpointLocation match {
      case Some(location) => query.option("checkpointLocation", location)
      case None => query
    }

    // Set query name if provided
    val finalQuery = options.queryName match {
      case Some(name) => queryWithCheckpoint.queryName(name)
      case None => queryWithCheckpoint
    }

    finalQuery.start()
  }

  /**
    * Process a single micro-batch by running analyzers and updating state.
    */
  private def processBatch(
      batchDF: DataFrame,
      batchId: Long,
      analyzers: Seq[Analyzer[_, Metric[_]]],
      stateProvider: StateProvider,
      options: StreamingAnalysisOptions)
    : Unit = {

    // Skip empty batches
    if (batchDF.isEmpty) {
      return
    }

    try {
      // Run analyzers on this batch, aggregating with previous state
      val analyzerContext = AnalysisRunner.run(
        data = batchDF,
        analysis = Analysis(analyzers),
        aggregateWith = Some(stateProvider),
        saveStatesWith = Some(stateProvider),
        storageLevelOfGroupedDataForMultiplePasses =
          options.storageLevelOfGroupedDataForMultiplePasses
      )

      // Save results to metrics repository if configured
      options.metricsRepository.foreach { repository =>
        options.resultKeyGenerator.foreach { keyGen =>
          val resultKey = keyGen(batchId)
          repository.save(resultKey, analyzerContext)
        }
      }

      // Invoke callback if provided
      options.onBatchComplete.foreach { callback =>
        callback(analyzerContext, batchId)
      }

    } catch {
      case e: Exception =>
        // Log the error but don't fail the streaming query
        println(s"Error processing batch $batchId: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}

/**
  * Builder for streaming analysis runs with a fluent API
  */
class StreamingAnalysisRunBuilder(streamingData: DataFrame) {

  private var analyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty
  private var stateProvider: Option[StateProvider] = None
  private var metricsRepository: Option[MetricsRepository] = None
  private var resultKeyGenerator: Option[Long => ResultKey] = None
  private var trigger: Trigger = Trigger.ProcessingTime("1 minute")
  private var checkpointLocation: Option[String] = None
  private var queryName: Option[String] = None
  private var onBatchComplete: Option[(AnalyzerContext, Long) => Unit] = None
  private var storageLevelOfGroupedDataForMultiplePasses: StorageLevel =
    StorageLevel.MEMORY_AND_DISK

  /**
    * Add a single analyzer to the analysis
    */
  def addAnalyzer(analyzer: Analyzer[_, Metric[_]]): StreamingAnalysisRunBuilder = {
    analyzers = analyzers :+ analyzer
    this
  }

  /**
    * Add multiple analyzers to the analysis
    */
  def addAnalyzers(newAnalyzers: Seq[Analyzer[_, Metric[_]]]): StreamingAnalysisRunBuilder = {
    analyzers = analyzers ++ newAnalyzers
    this
  }

  /**
    * Set the analysis to run
    */
  def withAnalysis(analysis: Analysis): StreamingAnalysisRunBuilder = {
    analyzers = analysis.analyzers
    this
  }

  /**
    * Set the state provider for maintaining state across batches
    */
  def useStateProvider(provider: StateProvider): StreamingAnalysisRunBuilder = {
    stateProvider = Some(provider)
    this
  }

  /**
    * Save metrics to a repository after each batch
    */
  def saveMetricsTo(
      repository: MetricsRepository,
      keyGenerator: Long => ResultKey): StreamingAnalysisRunBuilder = {
    metricsRepository = Some(repository)
    resultKeyGenerator = Some(keyGenerator)
    this
  }

  /**
    * Set the trigger for the streaming query
    */
  def withTrigger(newTrigger: Trigger): StreamingAnalysisRunBuilder = {
    trigger = newTrigger
    this
  }

  /**
    * Set the checkpoint location for the streaming query
    */
  def withCheckpointLocation(location: String): StreamingAnalysisRunBuilder = {
    checkpointLocation = Some(location)
    this
  }

  /**
    * Set the query name
    */
  def withQueryName(name: String): StreamingAnalysisRunBuilder = {
    queryName = Some(name)
    this
  }

  /**
    * Register a callback to be invoked after each batch is processed
    */
  def onBatchComplete(callback: (AnalyzerContext, Long) => Unit): StreamingAnalysisRunBuilder = {
    onBatchComplete = Some(callback)
    this
  }

  /**
    * Set the storage level for grouped data that must be accessed multiple times
    */
  def useStorageLevel(level: StorageLevel): StreamingAnalysisRunBuilder = {
    storageLevelOfGroupedDataForMultiplePasses = level
    this
  }

  /**
    * Start the streaming query
    */
  def start(): StreamingQuery = {
    val options = StreamingAnalysisOptions(
      stateProvider = stateProvider,
      metricsRepository = metricsRepository,
      resultKeyGenerator = resultKeyGenerator,
      trigger = trigger,
      checkpointLocation = checkpointLocation,
      queryName = queryName,
      onBatchComplete = onBatchComplete,
      storageLevelOfGroupedDataForMultiplePasses = storageLevelOfGroupedDataForMultiplePasses
    )

    StreamingAnalysisRunner.run(
      streamingData,
      Analysis(analyzers),
      options
    )
  }
}
