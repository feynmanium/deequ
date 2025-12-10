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

package com.amazon.deequ.quarantine

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.analyzers.{Analysis, Analyzer, StateLoader, StatePersister}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.{Constraint, ConstraintStatus}
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Engine for applying data quality checks with quarantine functionality.
  * Enables splitting data into valid and invalid streams based on row-level check results.
  *
  * Example usage:
  * {{{
  * val result = QuarantineEngine.runWithQuarantine(
  *   data = df,
  *   checks = Seq(check1, check2),
  *   rowLevelConfig = RowLevelConfig(markInvalidRows = true),
  *   quarantineConfig = QuarantineConfig(quarantineOnError = true)
  * )
  *
  * // Write valid data to production table
  * result.validData.write.saveAsTable("production.clean_data")
  *
  * // Write quarantined data for review
  * result.quarantinedData.write.saveAsTable("production.quarantine")
  * }}}
  */
object QuarantineEngine {

  /**
    * Run checks with quarantine functionality, splitting data into valid and invalid datasets.
    *
    * @param data Data to validate
    * @param checks Checks to apply
    * @param requiredAnalyzers Optional additional analyzers to run
    * @param aggregateWith Optional state loader for incremental computation
    * @param saveStatesWith Optional state persister
    * @param rowLevelConfig Configuration for row-level marking
    * @param quarantineConfig Configuration for quarantine behavior
    * @return QuarantineResult with valid data, quarantined data, and verification results
    */
  def runWithQuarantine(
      data: DataFrame,
      checks: Seq[Check],
      requiredAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty,
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None,
      rowLevelConfig: RowLevelConfig = RowLevelConfig(),
      quarantineConfig: QuarantineConfig = QuarantineConfig())
    : QuarantineResult = {

    // Get row-level constraint column names from checks
    val rowLevelColumns = checks.flatMap(_.getRowLevelConstraintColumnNames()).distinct

    // Run verification with row-level results
    val verificationResult = VerificationSuite()
      .onData(data)
      .addChecks(checks)
      .addRequiredAnalyzers(requiredAnalyzers)
      .run()

    // If no row-level constraints, split based on verification result only
    if (rowLevelColumns.isEmpty) {
      val (valid, quarantined) = splitBasedOnStatus(
        data,
        verificationResult,
        quarantineConfig
      )
      return QuarantineResult(valid, quarantined, verificationResult)
    }

    // Build quarantine condition based on row-level results
    val quarantineCondition = buildQuarantineCondition(
      checks,
      rowLevelColumns,
      quarantineConfig
    )

    // Add metadata columns if requested
    var dataWithMetadata = data
    if (rowLevelConfig.markInvalidRows) {
      dataWithMetadata = addInvalidMarker(
        data,
        quarantineCondition,
        rowLevelConfig.invalidMarkerColumn
      )
    }

    if (quarantineConfig.addMetadataColumns) {
      dataWithMetadata = addMetadataColumns(
        dataWithMetadata,
        checks,
        quarantineConfig.metadataPrefix
      )
    }

    // Split into valid and quarantined datasets
    val validData = dataWithMetadata.filter(not(quarantineCondition))
    val quarantinedData = dataWithMetadata.filter(quarantineCondition)

    // Drop check columns if requested
    val finalValidData = if (rowLevelConfig.dropCheckColumns) {
      dropCheckResultColumns(validData, rowLevelColumns, rowLevelConfig, quarantineConfig)
    } else {
      validData
    }

    val finalQuarantinedData = if (rowLevelConfig.dropCheckColumns) {
      dropCheckResultColumns(quarantinedData, rowLevelColumns, rowLevelConfig, quarantineConfig)
    } else {
      quarantinedData
    }

    QuarantineResult(finalValidData, finalQuarantinedData, verificationResult)
  }

  /**
    * Build a Spark SQL condition that evaluates to true for rows that should be quarantined.
    */
  private def buildQuarantineCondition(
      checks: Seq[Check],
      rowLevelColumns: Seq[String],
      config: QuarantineConfig): Column = {

    // Group checks by level
    val errorChecks = checks.filter(_.level == CheckLevel.Error)
    val warningChecks = checks.filter(_.level == CheckLevel.Warning)

    // Build conditions for error-level checks
    val errorConditions = errorChecks.flatMap { check =>
      check.getRowLevelConstraintColumnNames().map { colName =>
        col(colName) === false // Failed constraint (false value means violation)
      }
    }

    // Build conditions for warning-level checks if configured
    val warningConditions = if (config.quarantineOnWarning) {
      warningChecks.flatMap { check =>
        check.getRowLevelConstraintColumnNames().map { colName =>
          col(colName) === false
        }
      }
    } else {
      Seq.empty
    }

    val allConditions = errorConditions ++ warningConditions

    if (allConditions.isEmpty) {
      lit(false) // Don't quarantine anything if no conditions
    } else {
      allConditions.reduce(_ or _) // Quarantine if ANY check failed
    }
  }

  /**
    * Add a marker column indicating whether the row is invalid.
    */
  private def addInvalidMarker(
      data: DataFrame,
      invalidCondition: Column,
      markerColumnName: String): DataFrame = {

    data.withColumn(markerColumnName, invalidCondition)
  }

  /**
    * Add metadata columns with check information.
    */
  private def addMetadataColumns(
      data: DataFrame,
      checks: Seq[Check],
      prefix: String): DataFrame = {

    var result = data

    // Add timestamp
    result = result.withColumn(s"${prefix}checked_at", current_timestamp())

    // Add check descriptions
    val checkNames = checks.map(_.description).mkString(", ")
    result = result.withColumn(s"${prefix}checks", lit(checkNames))

    result
  }

  /**
    * Drop check result columns from the DataFrame.
    */
  private def dropCheckResultColumns(
      data: DataFrame,
      rowLevelColumns: Seq[String],
      rowLevelConfig: RowLevelConfig,
      quarantineConfig: QuarantineConfig): DataFrame = {

    var result = data

    // Drop row-level constraint columns
    rowLevelColumns.foreach { colName =>
      if (data.columns.contains(colName)) {
        result = result.drop(colName)
      }
    }

    // Drop marker column if it exists
    if (data.columns.contains(rowLevelConfig.invalidMarkerColumn)) {
      result = result.drop(rowLevelConfig.invalidMarkerColumn)
    }

    result
  }

  /**
    * Split data based on overall verification status (for aggregate checks without row-level results).
    */
  private def splitBasedOnStatus(
      data: DataFrame,
      verificationResult: VerificationResult,
      config: QuarantineConfig): (DataFrame, DataFrame) = {

    val shouldQuarantine = verificationResult.status match {
      case CheckStatus.Error => config.quarantineOnError
      case CheckStatus.Warning => config.quarantineOnWarning
      case CheckStatus.Success => false
    }

    if (shouldQuarantine) {
      // Entire dataset is quarantined
      (data.sparkSession.emptyDataFrame, data)
    } else {
      // Entire dataset is valid
      (data, data.sparkSession.emptyDataFrame)
    }
  }

  /**
    * Get only valid rows from a DataFrame with check result columns.
    *
    * @param data DataFrame with check result columns
    * @param quarantineOnError If true, filter out rows that failed Error-level checks
    * @param quarantineOnWarning If true, also filter out rows that failed Warning-level checks
    * @param dropCheckColumns If true, remove check result columns from output
    * @return DataFrame containing only valid rows
    */
  def getValid(
      data: DataFrame,
      quarantineOnError: Boolean = true,
      quarantineOnWarning: Boolean = false,
      dropCheckColumns: Boolean = true): DataFrame = {

    // Find check result columns (typically named _deequ_check_*)
    val checkColumns = data.columns.filter(_.startsWith("_deequ_check_"))

    if (checkColumns.isEmpty) {
      return data
    }

    // Build filter condition (all checks must pass)
    val filterCondition = checkColumns
      .map(colName => col(colName) =!= false) // Not false (could be true or null)
      .reduce(_ and _)

    var result = data.filter(filterCondition)

    // Drop check columns if requested
    if (dropCheckColumns) {
      checkColumns.foreach { colName =>
        result = result.drop(colName)
      }
      // Also drop metadata columns
      data.columns.filter(_.startsWith("_deequ_")).foreach { colName =>
        result = result.drop(colName)
      }
    }

    result
  }

  /**
    * Get only invalid rows from a DataFrame with check result columns.
    *
    * @param data DataFrame with check result columns
    * @param errorOnly If true, return only rows that failed Error-level checks
    * @param dropCheckColumns If true, remove check result columns from output
    * @return DataFrame containing only invalid rows
    */
  def getInvalid(
      data: DataFrame,
      errorOnly: Boolean = true,
      dropCheckColumns: Boolean = false): DataFrame = {

    // Find check result columns
    val checkColumns = data.columns.filter(_.startsWith("_deequ_check_"))

    if (checkColumns.isEmpty) {
      return data.sparkSession.emptyDataFrame
    }

    // Build filter condition (any check failed)
    val filterCondition = checkColumns
      .map(colName => col(colName) === false)
      .reduce(_ or _)

    var result = data.filter(filterCondition)

    // Drop check columns if requested
    if (dropCheckColumns) {
      checkColumns.foreach { colName =>
        result = result.drop(colName)
      }
    }

    result
  }
}
