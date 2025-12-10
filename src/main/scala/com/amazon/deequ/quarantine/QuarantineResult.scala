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

import com.amazon.deequ.VerificationResult
import org.apache.spark.sql.DataFrame

/**
  * Result of applying data quality checks with quarantine functionality.
  * Splits data into valid and invalid (quarantined) datasets based on check results.
  *
  * @param validData DataFrame containing only rows that passed all Error-level checks
  * @param quarantinedData DataFrame containing rows that failed any Error-level checks
  * @param verificationResult The verification result with metrics and check outcomes
  */
case class QuarantineResult(
    validData: DataFrame,
    quarantinedData: DataFrame,
    verificationResult: VerificationResult)

/**
  * Configuration for row-level quality checking and marking.
  *
  * @param markInvalidRows If true, adds columns to track which checks failed per row
  * @param invalidMarkerColumn Name of the column to add indicating row validity (default: "_deequ_invalid")
  * @param checkFailureColumns If true, adds individual columns for each check showing pass/fail
  * @param dropCheckColumns If true, removes check result columns from final output
  */
case class RowLevelConfig(
    markInvalidRows: Boolean = true,
    invalidMarkerColumn: String = "_deequ_invalid",
    checkFailureColumns: Boolean = true,
    dropCheckColumns: Boolean = false)

/**
  * Configuration for quarantine behavior.
  *
  * @param quarantineOnError If true, quarantine rows failing Error-level checks
  * @param quarantineOnWarning If true, also quarantine rows failing Warning-level checks
  * @param addMetadataColumns If true, add metadata columns (check names, timestamps, etc.)
  * @param metadataPrefix Prefix for metadata columns (default: "_deequ_")
  */
case class QuarantineConfig(
    quarantineOnError: Boolean = true,
    quarantineOnWarning: Boolean = false,
    addMetadataColumns: Boolean = true,
    metadataPrefix: String = "_deequ_")
