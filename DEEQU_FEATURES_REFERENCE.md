# AWS Deequ - Complete Features & Design Patterns Reference

**Version**: Based on Deequ 2.x codebase analysis
**Last Updated**: 2025-11-16

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Abstractions](#core-abstractions)
3. [Complete Analyzer Catalog (42+)](#complete-analyzer-catalog)
4. [Complete Check & Constraint Catalog (60+)](#complete-check--constraint-catalog)
5. [Data Profiling Features](#data-profiling-features)
6. [Anomaly Detection](#anomaly-detection)
7. [Constraint Suggestion System](#constraint-suggestion-system)
8. [Metrics Repository](#metrics-repository)
9. [Advanced Features](#advanced-features)
10. [Design Patterns](#design-patterns)
11. [Package Structure](#package-structure)

---

## Architecture Overview

### Layered Architecture

```
┌─────────────────────────────────────────────────────┐
│  Entry Points: VerificationSuite, ColumnProfiler,   │
│         ConstraintSuggestionRunner                   │
├─────────────────────────────────────────────────────┤
│  Checks & Constraint Rules Layer (60+ constraints)  │
├─────────────────────────────────────────────────────┤
│  Analyzers Layer (42+ analyzers)                    │
├─────────────────────────────────────────────────────┤
│  State Management & Incremental Computation          │
├─────────────────────────────────────────────────────┤
│  Analysis Runner (Scan Sharing Optimization)         │
├─────────────────────────────────────────────────────┤
│  Apache Spark DataFrame Layer                        │
└─────────────────────────────────────────────────────┘
```

---

## Core Abstractions

### 1. State (Algebraic States)

**Purpose**: Enable incremental computation through commutative semi-groups

**Key Trait**:
```scala
trait State[S <: State[S]] {
  def sum(other: S): S  // Associative & commutative operation
}
```

**State Implementations**:

| State Type | Purpose | Properties Tracked |
|------------|---------|-------------------|
| `NumMatches` | Simple counter | count |
| `NumMatchesAndCount` | Ratio metrics | numMatches, count |
| `FrequenciesAndNumRows` | Frequency analysis | frequencies, numRows |
| `MeanState` | Average calculation | sum, count |
| `StandardDeviationState` | Stddev calculation | n, avg, m2 |
| `CorrelationState` | Correlation | n, xSum, ySum, xySum, xSqSum, ySqSum |
| `ApproxQuantileState` | Quantile estimation | percentile digest |
| `KLLState` | KLL sketch | KLL sketch data |
| `DataTypeHistogram` | Type distribution | type counts |
| `SumState` | Sum calculation | sum |
| `MinState` | Minimum value | min |
| `MaxState` | Maximum value | max |

**Design Benefit**: States can be combined across partitions/time periods:
```scala
// Incremental computation
val totalState = state_day1 + state_day2 + state_day3
val metric = analyzer.computeMetricFrom(totalState)
```

### 2. Analyzer

**Purpose**: Compute data quality metrics from DataFrames

**Hierarchy**:

```
Analyzer[S <: State[_], M <: Metric[_]]
├── ScanShareableAnalyzer
│   └── StandardScanShareableAnalyzer (produces DoubleMetric)
├── GroupingAnalyzer
│   ├── FrequencyBasedAnalyzer
│   └── ScanShareableFrequencyBasedAnalyzer
└── FilterableAnalyzer
```

**Core Methods**:
- `computeStateFrom(data: DataFrame): Option[S]` - Compute state from data
- `computeMetricFrom(state: Option[S]): M` - Convert state to metric
- `preconditions: Seq[StructType => Unit]` - Validation preconditions
- `aggregationFunctions()` - For scan sharing (ScanShareableAnalyzer only)

### 3. Check

**Purpose**: Group related constraints with a severity level

```scala
case class Check(
  level: CheckLevel.Value,      // Error or Warning
  description: String,
  constraints: Seq[Constraint]
)
```

**Check Levels**:
- `CheckLevel.Error` - Failures are critical, stop processing
- `CheckLevel.Warning` - Failures are logged but non-blocking

### 4. Constraint

**Purpose**: Assert conditions on computed metrics

```scala
trait Constraint {
  def evaluate(analysisResults: Map[Analyzer[_, Metric[_]], Metric[_]]): ConstraintResult
}
```

**Constraint Types**:
- `AnalysisBasedConstraint` - Based on analyzer results
- `NamedConstraint` - Decorator with custom name
- `RowLevelConstraint` - Produces per-row results
- `RowLevelAssertedConstraint` - With UDF assertion
- `RowLevelGroupedConstraint` - Grouped constraints
- `DatasetMatchConstraint` - Dataset comparison

### 5. Metric

**Purpose**: Encapsulate computed metric values

**Metric Types**:
- `DoubleMetric` - Numeric metrics (most common)
- `HistogramMetric` - Categorical distributions
- `KLLMetric` - KLL sketch with quantiles
- `HistogramBinnedMetric` - Binned numeric distributions

---

## Complete Analyzer Catalog

### Basic Statistics (11 Analyzers)

| Analyzer | Input | Output | State | Scan Shareable |
|----------|-------|--------|-------|----------------|
| `Size()` | Dataset | Row count | NumMatches | ✓ |
| `ColumnCount()` | Dataset | Column count | NumMatches | ✓ |
| `ColumnExists(column)` | Column | Existence bool | ColumnExistsState | ✓ |
| `Mean(column)` | Numeric column | Average | MeanState | ✓ |
| `Sum(column)` | Numeric column | Sum | SumState | ✓ |
| `Minimum(column)` | Comparable column | Min value | MinState | ✓ |
| `Maximum(column)` | Comparable column | Max value | MaxState | ✓ |
| `StandardDeviation(column)` | Numeric column | Std deviation | StdDevState | ✓ |
| `MinLength(column)` | String column | Min string length | MinState | ✓ |
| `MaxLength(column)` | String column | Max string length | MaxState | ✓ |
| `Variance(column)` | Numeric column | Variance | StdDevState | ✓ |

### Completeness & Uniqueness (7 Analyzers)

| Analyzer | Computes | Output Range | State |
|----------|----------|--------------|-------|
| `Completeness(column)` | Non-null ratio | [0, 1] | NumMatchesAndCount |
| `Uniqueness(columns)` | Unique values ratio | [0, 1] | FrequenciesAndNumRows |
| `Distinctness(columns)` | Distinct/total ratio | [0, 1] | FrequenciesAndNumRows |
| `UniqueValueRatio(columns)` | Unique/total ratio | [0, 1] | FrequenciesAndNumRows |
| `CountDistinct(columns)` | Exact distinct count | Integer | - |
| `ApproxCountDistinct(columns)` | Approx distinct (HLL++) | Integer | HLLState |
| `Entropy(columns)` | Shannon entropy | [0, ∞) | FrequenciesAndNumRows |

### Pattern & Compliance (3 Analyzers)

| Analyzer | Purpose | Output |
|----------|---------|--------|
| `Compliance(name, predicate)` | SQL predicate compliance | Ratio [0, 1] |
| `PatternMatch(column, regex)` | Regex pattern match ratio | Ratio [0, 1] |
| `DataType(column)` | Data type distribution | Histogram |

**Built-in Patterns** (via `Patterns` object):
- `Patterns.CREDITCARD` - Credit card numbers
- `Patterns.EMAIL` - Email addresses
- `Patterns.URL` - URLs (http/https)
- `Patterns.SOCIAL_SECURITY_NUMBER_US` - US SSN

### Distribution Analyzers (4 Analyzers)

| Analyzer | Purpose | Output Type |
|----------|---------|-------------|
| `Histogram(column, binningUdf)` | Categorical distribution | HistogramMetric |
| `HistogramBinned(column, maxBins)` | Numeric binned distribution | HistogramBinnedMetric |
| `MutualInformation(cols, bins)` | Mutual information | Double |
| `Entropy(column)` | Shannon entropy | Double |

**Histogram Features**:
- Automatic binning for numerics
- Custom binning UDFs
- Empty/null bin counting
- Frequency-based analysis

### Quantile Analyzers (4 Analyzers)

| Analyzer | Algorithm | Accuracy | Memory |
|----------|-----------|----------|--------|
| `ApproxQuantile(column, quantile)` | Percentile digest | Approx | O(1) |
| `ApproxQuantiles(column, quantiles)` | Percentile digest | Approx | O(1) |
| `ExactQuantile(column, quantile)` | Exact sorting | Exact | O(n) |
| `KLLSketch(column, kllParameters)` | KLL algorithm | Guaranteed error | O(k) |

**KLL Sketch Features**:
- Configurable sketch size
- Multiple quantiles from single sketch
- Guaranteed error bounds
- Mergeable across partitions

### Correlation & Association (2 Analyzers)

| Analyzer | Measures | Output Range |
|----------|----------|--------------|
| `Correlation(column1, column2)` | Pearson correlation | [-1, 1] |
| `MutualInformation(col1, col2)` | Information gain | [0, ∞) |

### Advanced Analyzers (11 Analyzers)

| Analyzer | Purpose | Custom Logic |
|----------|---------|--------------|
| `CustomSql(expression)` | Execute custom SQL | User-defined SQL |
| `CustomAggregator(aggExpr, column)` | Custom Spark aggregation | User-defined agg |
| `DatasetMatchAnalyzer(other, keyMapping)` | Dataset comparison | Match ratio |
| `Distance(column)` | Distance metrics | Various metrics |
| `RatioOfSums(col1, col2)` | Sum ratio | ratio |
| `ApproxDistinctness(columns)` | Approx distinctness | HLL-based |
| `DatasetSize()` | Dataset size in bytes | Bytes |
| `ColumnDataType(column)` | Column data type | Type string |
| `ColumnLength(column)` | Column length stats | Min/max/avg |
| `RowCount()` | Row count (alias for Size) | Count |
| `ComplianceByPartition(predicate, partitionBy)` | Partitioned compliance | Per-partition ratio |

---

## Complete Check & Constraint Catalog

### Entry Point

```scala
Check(level: CheckLevel, description: String)
  .constraint1()
  .constraint2()
  // ... chain multiple constraints
```

### Completeness Constraints (6 methods)

```scala
// Single column
.isComplete(column: String)
  // Asserts 100% completeness (no nulls)

.hasCompleteness(column: String, assertion: Double => Boolean)
  // Custom completeness threshold
  // Example: .hasCompleteness("email", _ >= 0.95)

// Multiple columns
.areComplete(columns: String*)
  // All columns must be 100% complete

.haveCompleteness(columns: Seq[String], assertion: Double => Boolean)
  // Combined completeness across columns

.areAnyComplete(columns: String*)
  // At least one column is complete

.haveAnyCompleteness(columns: Seq[String], assertion: Double => Boolean)
  // Any column meets completeness threshold
```

### Uniqueness Constraints (5 methods)

```scala
.isUnique(column: String)
  // Asserts 100% uniqueness

.areUnique(columns: String*)
  // Combined columns are unique (composite key)

.isPrimaryKey(column: String, columns: String*)
  // Complete + Unique (primary key constraint)

.hasUniqueness(columns: Seq[String], assertion: Double => Boolean)
  // Custom uniqueness threshold

.hasDistinctness(columns: Seq[String], assertion: Double => Boolean)
  // Distinct values ratio
```

### Distinct Value Constraints (3 methods)

```scala
.hasNumberOfDistinctValues(column: String, assertion: Long => Boolean)
  // Exact count of distinct values

.hasUniqueValueRatio(columns: Seq[String], assertion: Double => Boolean)
  // Ratio of unique values

.hasApproxCountDistinct(column: String, assertion: Long => Boolean)
  // Approximate distinct count (HyperLogLog++)
```

### Numeric Range Constraints (7 methods)

```scala
.hasMin(column: String, assertion: Double => Boolean)
  // Minimum value constraint
  // Example: .hasMin("age", _ >= 0)

.hasMax(column: String, assertion: Double => Boolean)
  // Maximum value constraint
  // Example: .hasMax("age", _ <= 120)

.hasMean(column: String, assertion: Double => Boolean)
  // Mean value constraint

.hasSum(column: String, assertion: Double => Boolean)
  // Sum constraint

.hasStandardDeviation(column: String, assertion: Double => Boolean)
  // Standard deviation constraint

.isNonNegative(column: String)
  // Asserts min >= 0

.isPositive(column: String)
  // Asserts min > 0
```

### Quantile Constraints (2 methods)

```scala
.hasApproxQuantile(column: String, quantile: Double, assertion: Double => Boolean)
  // Approximate quantile
  // Example: .hasApproxQuantile("price", 0.5, _ <= 100) // median

.hasExactQuantile(column: String, quantile: Double, assertion: Double => Boolean)
  // Exact quantile (slower)
```

### String Length Constraints (2 methods)

```scala
.hasMinLength(column: String, assertion: Double => Boolean)
  // Minimum string length

.hasMaxLength(column: String, assertion: Double => Boolean)
  // Maximum string length
```

### Pattern Matching Constraints (5 methods)

```scala
.hasPattern(column: String, regex: Regex, assertion: Double => Boolean)
  // Custom regex pattern match ratio

.containsCreditCardNumber(column: String, assertion: Double => Boolean = _ == 1.0)
  // Credit card pattern detection

.containsEmail(column: String, assertion: Double => Boolean = _ == 1.0)
  // Email pattern detection

.containsURL(column: String, assertion: Double => Boolean = _ == 1.0)
  // URL pattern detection

.containsSocialSecurityNumber(column: String, assertion: Double => Boolean = _ == 1.0)
  // US SSN pattern detection
```

### Data Type Constraints (2 methods)

```scala
.hasDataType(column: String, dataType: ConstrainableDataType, assertion: Double => Boolean)
  // Data type compliance
  // Types: Null, Fractional, Integral, Boolean, String, Numeric

.hasColumn(columnName: String)
  // Column existence check
```

**ConstrainableDataTypes**:
- `ConstrainableDataTypes.Null`
- `ConstrainableDataTypes.Fractional` (Float, Double, Decimal)
- `ConstrainableDataTypes.Integral` (Byte, Short, Int, Long)
- `ConstrainableDataTypes.Boolean`
- `ConstrainableDataTypes.String`
- `ConstrainableDataTypes.Numeric` (Fractional + Integral)

### Distribution Constraints (5 methods)

```scala
.hasHistogramValues(column: String, assertion: Distribution => Boolean)
  // Histogram distribution assertion

.hasHistogramBinnedValues(column: String, assertion: Distribution => Boolean)
  // Binned histogram assertion

.hasHistogramBinnedBins(column: String, assertion: Long => Boolean)
  // Number of bins assertion

.hasEntropy(column: String, assertion: Double => Boolean)
  // Shannon entropy assertion

.hasMutualInformation(columnA: String, columnB: String, assertion: Double => Boolean)
  // Mutual information between columns
```

### KLL Sketch Constraints (1 method)

```scala
.kllSketchSatisfies(column: String, assertion: KLLSketch => Boolean)
  // Custom KLL sketch assertion
  // Can check quantiles, min, max from sketch
```

### Compliance Constraints (10 methods)

```scala
.satisfies(predicate: String, constraintName: String, assertion: Double => Boolean)
  // Custom SQL predicate compliance
  // Example: .satisfies("price > 0 AND price < 1000", "price range", _ >= 0.95)

// Comparison operators
.isLessThan(columnA: String, columnB: String, assertion: Double => Boolean = _ == 1.0)
.isLessThanOrEqualTo(columnA: String, columnB: String, assertion: Double => Boolean = _ == 1.0)
.isGreaterThan(columnA: String, columnB: String, assertion: Double => Boolean = _ == 1.0)
.isGreaterThanOrEqualTo(columnA: String, columnB: String, assertion: Double => Boolean = _ == 1.0)

// Value set constraints
.isContainedIn(column: String, allowedValues: Array[String])
  // All values must be in allowed set

.isContainedIn(column: String, lowerBound: Double, upperBound: Double,
               includeLowerBound: Boolean = true, includeUpperBound: Boolean = true)
  // All values must be in numeric range
```

### Correlation Constraints (1 method)

```scala
.hasCorrelation(columnA: String, columnB: String, assertion: Double => Boolean)
  // Pearson correlation coefficient
  // Example: .hasCorrelation("feature1", "feature2", c => Math.abs(c) < 0.9)
```

### Custom SQL Constraints (1 method)

```scala
.customSql(expression: String, assertion: Double => Boolean)
  // Execute custom SQL expression
  // Example: .customSql("SELECT AVG(CASE WHEN price > 100 THEN 1 ELSE 0 END) FROM __THIS__", _ >= 0.5)
```

### Dataset Comparison Constraints (1 method)

```scala
.doesDatasetMatch(
  otherDataFrame: DataFrame,
  keyColumnMappings: Map[String, String],
  assertion: Double => Boolean,
  matchColumnMappings: Option[Map[String, String]] = None
)
  // Compare with another dataset
  // Returns ratio of matching rows
```

### Anomaly Detection Constraints (1 method)

```scala
.isNewestPointNonAnomalous(
  metricsRepository: MetricsRepository,
  anomalyDetectionStrategy: AnomalyDetectionStrategy,
  analyzer: Analyzer[_, Metric[_]],
  withTagValues: Map[String, String] = Map.empty,
  afterDate: Option[Long] = None,
  beforeDate: Option[Long] = None
)
  // Detect anomalies in time series
```

### Size Constraints (1 method)

```scala
.hasSize(assertion: Long => Boolean)
  // Row count assertion
  // Example: .hasSize(_ > 1000)
```

---

## Data Profiling Features

### ColumnProfiler

**Purpose**: Efficient 3-pass profiling of large datasets

**API**:
```scala
ColumnProfiler
  .onData(dataFrame)
  .restrictToColumns(columnNames)
  .setLowCardinalityHistogramThreshold(120)
  .setKLLProfiling(enabled = true)
  .setPredefinedTypes(Map("column" -> String))
  .run()
```

### Profiling Strategy

**Pass 1: Generic Statistics** (Single scan)
- Number of records
- Data type detection (for string columns)
- Approximate distinct values (HyperLogLog++)
- Completeness
- Min/Max string length (for string columns)

**Pass 2: Numeric Statistics** (For numeric columns only)
- Mean
- Standard deviation
- Minimum
- Maximum
- Sum
- KLL Sketch (optional)
- Approximate percentiles (from KLL: 0.05, 0.25, 0.5, 0.75, 0.95)

**Pass 3: Categorical Statistics** (For low-cardinality columns)
- Exact histograms
- Default threshold: ≤120 distinct values
- Configurable via `setLowCardinalityHistogramThreshold()`

### Profile Types

**NumericColumnProfile**:
```scala
case class NumericColumnProfile(
  name: String,
  completeness: Double,
  approximateNumDistinctValues: Long,
  dataType: DataTypeInstances.Value,
  mean: Option[Double],
  maximum: Option[Double],
  minimum: Option[Double],
  sum: Option[Double],
  stdDev: Option[Double],
  approxPercentiles: Option[Seq[Double]],
  kllMetrics: Option[KLLMetric],
  histogram: Option[Distribution]
)
```

**StringColumnProfile**:
```scala
case class StringColumnProfile(
  name: String,
  completeness: Double,
  approximateNumDistinctValues: Long,
  dataType: DataTypeInstances.Value,
  minLength: Option[Distribution],
  maxLength: Option[Distribution],
  histogram: Option[Distribution]
)
```

**StandardColumnProfile**:
```scala
case class StandardColumnProfile(
  name: String,
  completeness: Double,
  approximateNumDistinctValues: Long,
  dataType: DataTypeInstances.Value
)
```

### Configuration Options

```scala
// Restrict to specific columns
.restrictToColumns(Seq("col1", "col2"))

// Set histogram threshold (default: 120)
.setLowCardinalityHistogramThreshold(100)

// Enable KLL profiling (default: false)
.setKLLProfiling(enabled = true,
  kllParameters = Some(KLLParameters(sketchSize = 2048, shrinkingFactor = 0.64)))

// Override type detection
.setPredefinedTypes(Map(
  "id" -> DataTypeInstances.Integral,
  "price" -> DataTypeInstances.Fractional,
  "name" -> DataTypeInstances.String
))
```

---

## Anomaly Detection

### Anomaly Detection Strategies

#### 1. OnlineNormalStrategy

**Assumes**: Normal distribution
**Method**: Running mean and standard deviation

```scala
OnlineNormalStrategy(
  lowerDeviationFactor: Option[Double] = None,  // Default: 3.0
  upperDeviationFactor: Option[Double] = None,  // Default: 3.0
  ignoreStartPercentage: Double = 0.1,           // Ignore first 10%
  ignoreAnomalies: Boolean = false               // Include anomalies in calculation
)
```

**Detection**: Value is anomalous if outside `mean ± factor * stddev`

**Use Case**: Metrics that follow normal distribution (e.g., mean response time)

#### 2. BatchNormalStrategy

**Assumes**: Normal distribution
**Method**: Pre-computed mean and stddev

```scala
BatchNormalStrategy(
  lowerDeviationFactor: Option[Double] = None,
  upperDeviationFactor: Option[Double] = None
)
```

**Use Case**: When historical statistics are known

#### 3. SimpleThresholdStrategy

**Method**: Fixed upper/lower bounds

```scala
SimpleThresholdStrategy(
  lowerBound: Double = Double.MinValue,
  upperBound: Double = Double.MaxValue
)
```

**Use Case**: Known acceptable ranges (e.g., percentage must be [0, 1])

#### 4. AbsoluteChangeStrategy

**Method**: Detects absolute change from previous value

```scala
AbsoluteChangeStrategy(
  maxRateIncrease: Option[Double] = None,
  maxRateDecrease: Option[Double] = None,
  order: Int = 1  // Compare with N values back
)
```

**Detection**: `|value[t] - value[t-1]| > threshold`

**Use Case**: Metrics that shouldn't jump suddenly (e.g., user count)

#### 5. RateOfChangeStrategy

**Method**: Detects rate of change

```scala
RateOfChangeStrategy(
  maxRateIncrease: Option[Double] = None,
  maxRateDecrease: Option[Double] = None,
  order: Int = 1
)
```

**Detection**: `(value[t] - value[t-1]) > threshold`

**Use Case**: Metrics with expected growth rates

#### 6. RelativeRateOfChangeStrategy

**Method**: Detects relative (percentage) change

```scala
RelativeRateOfChangeStrategy(
  maxRateIncrease: Option[Double] = None,
  maxRateDecrease: Option[Double] = None,
  order: Int = 1
)
```

**Detection**: `(value[t] - value[t-1]) / value[t-1] > threshold`

**Use Case**: Metrics where percentage change matters (e.g., revenue)

### Usage Pattern

```scala
val repository = FileSystemMetricsRepository(spark, "metrics/")

val verificationResult = VerificationSuite()
  .onData(dataFrame)
  .useRepository(repository)
  .addCheck(
    Check(CheckLevel.Warning, "Anomaly Detection")
      .isNewestPointNonAnomalous(
        repository,
        OnlineNormalStrategy(upperDeviationFactor = Some(3.0)),
        Size(),
        withTagValues = Map("dataset" -> "daily_sales", "region" -> "us-east"),
        afterDate = Some(timestampSevenDaysAgo),
        beforeDate = Some(timestampNow)
      )
  )
  .run()
```

### Seasonal Anomaly Detection

**Location**: `com.amazon.deequ.anomalydetection.seasonal`

**Features**:
- Seasonal decomposition
- Trend detection
- Seasonality-adjusted anomaly detection

---

## Constraint Suggestion System

### ConstraintSuggestionRunner

**Purpose**: Automatically generate constraint suggestions from data profiling

**API**:
```scala
val suggestionResult = ConstraintSuggestionRunner()
  .onData(dataFrame)
  .addConstraintRules(Rules.DEFAULT)
  .useSparkSession(spark)
  .useTrainTestSplitWithTestsetRatio(0.2)
  .run()
```

### Constraint Suggestion Rules

#### DEFAULT Rules

1. **CompleteIfCompleteRule**
   - Threshold: > 90% complete
   - Suggests: `.isComplete(column)`

2. **RetainCompletenessRule**
   - Suggests: `.hasCompleteness(column, _ >= currentCompleteness * 0.9)`
   - Maintains current completeness level

3. **RetainTypeRule**
   - Analyzes data type distribution
   - Suggests: `.hasDataType(column, detectedType, _ >= 0.9)`

4. **CategoricalRangeRule**
   - For low-cardinality columns
   - Suggests: `.isContainedIn(column, observedValues)`

5. **FractionalCategoricalRangeRule**
   - For fractional categorical columns
   - Suggests categorical constraints with fractional thresholds

6. **NonNegativeNumbersRule**
   - If min >= 0
   - Suggests: `.isNonNegative(column)`

#### STRING Rules

7. **HasMinLength**
   - Suggests: `.hasMinLength(column, _ >= observedMinLength)`

8. **HasMaxLength**
   - Suggests: `.hasMaxLength(column, _ <= observedMaxLength)`

#### NUMERICAL Rules

9. **HasMax**
   - Suggests: `.hasMax(column, _ <= observedMax * 1.1)`

10. **HasMin**
    - Suggests: `.hasMin(column, _ >= observedMin * 0.9)`

11. **HasMean**
    - Suggests: `.hasMean(column, mean => Math.abs(mean - observedMean) < tolerance)`

12. **HasStandardDeviation**
    - Suggests: `.hasStandardDeviation(column, _ < observedStdDev * 2.0)`

#### COMMON Rules

13. **UniqueIfApproximatelyUniqueRule**
    - Threshold: > 99% unique
    - Suggests: `.isUnique(column)`

#### EXTENDED Rules

Combination of: DEFAULT + STRING + NUMERICAL + COMMON

### Train/Test Split

**Purpose**: Validate suggested constraints

```scala
.useTrainTestSplitWithTestsetRatio(0.2)  // 80% train, 20% test
```

**Process**:
1. Split data into train/test
2. Profile training set
3. Generate constraint suggestions
4. Evaluate on test set
5. Return suggestions with test results

### Suggestion Output

```scala
case class ConstraintSuggestion(
  constraint: Constraint,
  columnName: String,
  currentValue: String,
  description: String,
  codeForConstraint: String
)
```

**Export to JSON**:
```scala
import com.amazon.deequ.suggestions.ConstraintSuggestionResult._

val jsonOutput = suggestionResult.toJson()
```

---

## Metrics Repository

### Repository Trait

```scala
trait MetricsRepository {
  def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit
  def loadByKey(resultKey: ResultKey): Option[AnalyzerContext]
  def load(): MetricsRepositoryMultipleResultsLoader
}
```

### ResultKey

```scala
case class ResultKey(
  dataSetDate: Long,           // Unix timestamp
  tags: Map[String, String]    // Custom tags
)
```

**Tags** allow flexible organization:
```scala
Map(
  "dataset" -> "sales",
  "region" -> "us-east",
  "environment" -> "production"
)
```

### Repository Implementations

#### 1. InMemoryMetricsRepository

**Purpose**: Testing and development

```scala
val repository = new InMemoryMetricsRepository()
```

**Storage**: `ConcurrentHashMap[ResultKey, AnalyzerContext]`

**Persistence**: None (volatile)

#### 2. FileSystemMetricsRepository

**Purpose**: Production persistence to distributed file systems

```scala
val repository = FileSystemMetricsRepository(
  spark,
  path = "s3://bucket/deequ/metrics/"
)
```

**Supported File Systems**:
- HDFS: `hdfs://namenode/path/`
- S3: `s3://bucket/path/`
- Local: `file:///local/path/`

**Storage Format**: JSON (one file per ResultKey)

**Features**:
- Overwrite or append modes
- Automatic serialization/deserialization
- Supports all metric types

#### 3. SparkMetricsRepository

**Purpose**: Store metrics as Spark tables

```scala
val repository = new SparkMetricsRepository(
  spark,
  tableName = "deequ_metrics"
)
```

**Storage**: Any Spark-supported table format (Parquet, ORC, Delta, etc.)

### Querying Metrics

**Single Result**:
```scala
val resultKey = ResultKey(timestamp, Map("dataset" -> "sales"))
val context: Option[AnalyzerContext] = repository.loadByKey(resultKey)
```

**Multiple Results with Filters**:
```scala
val results = repository.load()
  .forAnalyzers(Seq(Size(), Completeness("email")))
  .after(timestampSevenDaysAgo)
  .before(timestampNow)
  .withTagValues(Map("environment" -> "production"))
  .get()
```

### Reusing Existing Metrics

**Avoid recomputation**:
```scala
VerificationSuite()
  .onData(dataFrame)
  .useRepository(repository)
  .reuseExistingResultsForKey(resultKey)
  .addCheck(check)
  .run()
```

**Behavior**:
- Loads existing metrics from repository
- Only computes missing analyzers
- Merges new results with existing

### Saving Results

**Save with key**:
```scala
VerificationSuite()
  .onData(dataFrame)
  .useRepository(repository)
  .saveOrAppendResult(resultKey)
  .addCheck(check)
  .run()
```

**Modes**:
- `saveOrAppendResult()` - Append to existing results
- `overwriteOutputFiles()` - Replace existing results

---

## Advanced Features

### 1. Incremental Computation

**Purpose**: Process data incrementally, combining states across time periods

**State Providers**:

#### InMemoryStateProvider
```scala
val stateProvider = InMemoryStateProvider()
```

#### HdfsStateProvider
```scala
val stateProvider = HdfsStateProvider(
  spark,
  path = "s3://bucket/deequ/states/"
)
```

**Usage Pattern**:

```scala
// Day 1
val analysis1 = Analysis().addAnalyzer(Size())
val state1 = AnalysisRunner.run(
  data = day1Data,
  analysis = analysis1,
  saveStatesWith = Some(stateProvider)
)

// Day 2 - aggregate with previous state
val state2 = AnalysisRunner.run(
  data = day2Data,
  analysis = analysis1,
  aggregateWith = Some(stateProvider)
)

// state2 contains Size analyzer with combined count from both days
```

**Benefits**:
- Process data in chunks
- Combine results across partitions
- Update metrics without reprocessing all data

### 2. Partitioning Support

**Purpose**: Distribute computation across data partitions

**Run on Aggregated States**:
```scala
val aggregatedStates = InMemoryStateProvider()

VerificationSuite.runOnAggregatedStates(
  dataFrame.schema,
  checks,
  stateLoaders = Seq(partition1States, partition2States, partition3States),
  saveStatesWith = Some(aggregatedStates),
  metricsRepository = Some(repository),
  saveOrAppendResultsWithKey = Some(resultKey)
)
```

**Use Cases**:
- Partition data by date
- Compute states for each partition independently
- Aggregate states to get overall metrics
- Run checks on aggregated states

**Example Workflow**:
```scala
// 1. Process each partition
partitions.foreach { partition =>
  AnalysisRunner.run(
    data = partition.data,
    analysis = analysis,
    saveStatesWith = Some(partitionStateProvider(partition.id))
  )
}

// 2. Run verification on aggregated states
val partitionStates = partitions.map(p => partitionStateProvider(p.id))
VerificationSuite.runOnAggregatedStates(
  schema,
  checks,
  stateLoaders = partitionStates
)
```

### 3. Scan Sharing Optimization

**Purpose**: Minimize data passes through automatic optimization

**AnalysisRunner Strategy**:

1. **Group Analyzers**:
   - Scan-shareable analyzers (can share single scan)
   - Grouping analyzers (grouped by grouping columns)
   - KLL sketches (separate pass for memory efficiency)
   - Non-shareable analyzers (individual scans)

2. **Optimize Execution**:
   - Combine aggregation functions from scan-shareable analyzers
   - Single pass computes all aggregations
   - Result extraction uses column offsets

3. **Cache Management**:
   - Cache grouped data if multiple grouping keys
   - Configurable `StorageLevel`

**Example**:

Without optimization (10 scans):
```scala
Size(), Completeness("a"), Mean("a"), Completeness("b"), Mean("b")
// → 5 individual scans
```

With optimization (1 scan):
```scala
// Combines all into single aggregation:
// SELECT COUNT(*), COUNT(a), SUM(a), COUNT(b), SUM(b) FROM data
```

**Configuration**:
```scala
AnalysisRunner.run(
  data = dataFrame,
  analysis = analysis,
  aggregateWith = stateProvider,
  saveStatesWith = stateProvider,
  storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK
)
```

### 4. Row-Level Results

**Purpose**: Get detailed per-row constraint evaluation

**AnalyzerOptions**:
```scala
case class AnalyzerOptions(
  nullBehavior: NullBehavior.Value = NullBehavior.Ignore,
  filteredRowOutcome: FilteredRowOutcome.Value = FilteredRowOutcome.TRUE
)
```

**NullBehavior**:
- `Ignore` - Skip nulls (default)
- `EmptyString` - Treat nulls as empty strings
- `Fail` - Fail on nulls

**FilteredRowOutcome** (for WHERE clauses):
- `TRUE` - Filtered rows count as pass
- `NULL` - Filtered rows are null in output

**Row-Level Constraints**:

```scala
val check = Check(CheckLevel.Error, "row level checks")
  .hasCompleteness("email", _ == 1.0,
    analyzerOptions = Some(AnalyzerOptions(NullBehavior.Fail)))

// After run, access row-level results
val verificationResult = VerificationSuite()
  .onData(dataFrame)
  .addCheck(check)
  .run()

// Get column names for row-level constraints
val columnNames = check.getRowLevelConstraintColumnNames()
// → Seq("Completeness-email")

// DataFrame now has additional boolean column indicating pass/fail per row
```

**Use Cases**:
- Filter out bad rows
- Generate data quality reports per row
- Debug constraint failures
- Audit trail

### 5. Custom Analyzers

#### CustomSql

**Purpose**: Execute arbitrary SQL queries

```scala
CustomSql(
  expression = "SELECT AVG(CASE WHEN price > 100 THEN 1 ELSE 0 END) FROM __THIS__"
)
```

**`__THIS__`**: Placeholder for current DataFrame

**Returns**: `DoubleMetric` with query result

**Example Use Cases**:
- Complex business logic
- Multi-column calculations
- Conditional aggregations

#### CustomAggregator

**Purpose**: Use Spark aggregation expressions

```scala
CustomAggregator(
  aggregatorExpr = expr("percentile_approx(latency, 0.99)"),
  columnName = "latency"
)
```

**Benefits**:
- Leverage Spark's built-in aggregations
- Custom UDAFs
- Performance optimization

### 6. Dataset Comparison

**DatasetMatchAnalyzer**:
```scala
DatasetMatchAnalyzer(
  otherDataFrame: DataFrame,
  keyMappings: Map[String, String],
  matchColumnMappings: Option[Map[String, String]] = None
)
```

**Parameters**:
- `keyMappings`: Join keys mapping (`thisColumn -> otherColumn`)
- `matchColumnMappings`: Columns to compare for equality

**Returns**: Ratio of matching rows [0, 1]

**Example**:
```scala
val analyzer = DatasetMatchAnalyzer(
  referenceData,
  keyMappings = Map("id" -> "customer_id"),
  matchColumnMappings = Some(Map(
    "name" -> "customer_name",
    "email" -> "customer_email"
  ))
)

// In check
Check(CheckLevel.Error, "sync check")
  .doesDatasetMatch(referenceData,
    Map("id" -> "customer_id"),
    _ >= 0.95,  // 95% of rows must match
    Some(Map("name" -> "customer_name"))
  )
```

**Use Cases**:
- Data synchronization validation
- ETL output verification
- Regression testing

**ReferentialIntegrity**:
```scala
ReferentialIntegrity.isIntact(
  childDataFrame,
  childKey = Seq("order_id"),
  parentDataFrame,
  parentKey = Seq("id")
)
```

Validates foreign key relationships.

### 7. Schema Validation

**RowLevelSchemaValidator**:

```scala
import com.amazon.deequ.schema.RowLevelSchemaValidator

val validationResult = RowLevelSchemaValidator.validate(
  dataFrame,
  expectedSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("email", StringType, nullable = true)
  ))
)
```

**Features**:
- Column presence validation
- Data type validation
- Nullability validation
- Row-level validation results

### 8. DQDL (Data Quality Definition Language)

**Purpose**: Define data quality rules in a declarative, readable format

**Example**:
```scala
val ruleset = """
Rules=[
  IsUnique "id",
  RowCount < 10000,
  Completeness "email" > 0.95,
  Uniqueness "email" = 1.0,
  Mean "price" between 10 and 1000,
  StandardDeviation "price" < 100,
  IsPrimaryKey "customer_id"
]
"""

val results = EvaluateDataQuality.process(dataFrame, ruleset)
results.show()
```

**Supported Rules**:
- `RowCount`: Row count constraints
- `Completeness`: Completeness constraints
- `IsComplete`: Column completeness
- `Uniqueness`: Uniqueness constraints
- `IsUnique`: Column uniqueness
- `IsPrimaryKey`: Primary key constraint
- `ColumnCorrelation`: Correlation constraints
- `DistinctValuesCount`: Distinct count
- `Entropy`: Entropy constraints
- `Mean`: Mean constraints
- `StandardDeviation`: Stddev constraints
- `Sum`: Sum constraints
- `UniqueValueRatio`: Unique ratio
- `CustomSql`: Custom SQL queries
- `ColumnLength`: String length constraints
- `ColumnExists`: Column existence

**Output**: DataFrame with rule evaluation results

### 9. Performance Optimization

#### Storage Level Configuration

```scala
import org.apache.spark.storage.StorageLevel

AnalysisRunner.run(
  data = dataFrame,
  analysis = analysis,
  storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK_SER
)
```

**Storage Levels**:
- `MEMORY_ONLY` - Fast, high memory
- `MEMORY_AND_DISK` - Spill to disk
- `MEMORY_AND_DISK_SER` - Serialized (lower memory)
- `DISK_ONLY` - Slow, no memory usage

#### Preconditions

Analyzers validate schema before execution:

```scala
// Completeness preconditions
def preconditions: Seq[StructType => Unit] = {
  hasColumn(column) :: Nil
}
```

**Benefits**:
- Fail fast on schema issues
- Clear error messages
- Prevent wasted computation

### 10. Distributed Execution

**Spark Integration**:
- Native Spark DataFrame operations
- Catalyst optimizer integration
- Distributed aggregations
- Partition-aware processing

**Best Practices**:
```scala
// Repartition for better parallelism
dataFrame.repartition(numPartitions)

// Persist if running multiple checks
dataFrame.cache()

// Use broadcast for small lookup tables
dataFrame.join(broadcast(lookupTable), "key")
```

---

## Design Patterns

### 1. Builder Pattern (Fluent API)

**VerificationRunBuilder**:
```scala
VerificationSuite()
  .onData(dataFrame)
  .addCheck(check1)
  .addCheck(check2)
  .useRepository(repository)
  .saveOrAppendResult(resultKey)
  .reuseExistingResultsForKey(resultKey)
  .addRequiredAnalyzers(Seq(Size()))
  .run()
```

**Benefits**:
- Readable, self-documenting code
- Optional parameters
- Method chaining
- Immutable builders (functional style)

### 2. Strategy Pattern

**Anomaly Detection**:
```scala
trait AnomalyDetectionStrategy {
  def detect(
    dataSeries: Vector[Double],
    searchInterval: (Int, Int)
  ): Seq[(Int, Anomaly)]
}
```

**Implementations**:
- `OnlineNormalStrategy`
- `BatchNormalStrategy`
- `SimpleThresholdStrategy`
- `AbsoluteChangeStrategy`
- `RateOfChangeStrategy`
- `RelativeRateOfChangeStrategy`

**Benefits**:
- Pluggable algorithms
- Easy to add new strategies
- Strategy selection at runtime

### 3. Template Method Pattern

**Analyzer**:
```scala
trait Analyzer[S <: State[_], +M <: Metric[_]] {
  // Template method
  def calculate(data: DataFrame,
                aggregateWith: Option[StateLoader] = None,
                saveStatesWith: Option[StatePersister] = None): M = {
    preconditions.foreach(_(data.schema))

    val state = aggregateWith match {
      case Some(loader) =>
        val loadedState = loader.load[S](this)
        val computedState = computeStateFrom(data)
        loadedState.map(s => s.sum(computedState.get))
      case None =>
        computeStateFrom(data)
    }

    saveStatesWith.foreach(_.persist(this, state.get))
    computeMetricFrom(state)
  }

  // Abstract methods
  def computeStateFrom(data: DataFrame): Option[S]
  def computeMetricFrom(state: Option[S]): M
  def preconditions: Seq[StructType => Unit]
}
```

**Benefits**:
- Consistent execution flow
- Hooks for customization
- Code reuse

### 4. Decorator Pattern

**Constraints**:
```scala
class ConstraintDecorator(protected val _inner: Constraint) extends Constraint

class NamedConstraint(constraint: Constraint, name: String)
  extends ConstraintDecorator(constraint)

class RowLevelConstraint(constraint: Constraint, name: String, columnName: String)
  extends NamedConstraint(constraint, name)
```

**Benefits**:
- Add functionality without modifying original
- Compose decorators
- Open/Closed principle

### 5. Repository Pattern

**MetricsRepository**:
```scala
trait MetricsRepository {
  def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit
  def loadByKey(resultKey: ResultKey): Option[AnalyzerContext]
  def load(): MetricsRepositoryMultipleResultsLoader
}
```

**Implementations**:
- `InMemoryMetricsRepository`
- `FileSystemMetricsRepository`
- `SparkMetricsRepository`

**Benefits**:
- Abstract persistence layer
- Swappable implementations
- Testability

### 6. State Pattern (Algebraic)

**Commutative Semi-Group**:
```scala
trait State[S <: State[S]] {
  def sum(other: S): S  // Associative & commutative
}
```

**Properties**:
- **Associative**: `(a + b) + c = a + (b + c)`
- **Commutative**: `a + b = b + a`
- **Identity**: Optional zero element

**Benefits**:
- Incremental computation
- Partition-friendly
- Order-independent aggregation
- Distributed computation

**Example**:
```scala
case class NumMatches(count: Long) extends State[NumMatches] {
  override def sum(other: NumMatches): NumMatches = {
    NumMatches(count + other.count)
  }
}

// Can aggregate in any order
val total = state1 + state2 + state3
// = (state1 + state2) + state3
// = state1 + (state2 + state3)
// = state2 + state1 + state3
```

### 7. Factory Pattern

**Constraint Factory**:
```scala
object Constraint {
  def sizeConstraint(assertion: Long => Boolean): Constraint
  def completenessConstraint(column: String, assertion: Double => Boolean): Constraint
  def uniquenessConstraint(columns: Seq[String], assertion: Double => Boolean): Constraint
  // ... 40+ factory methods
}
```

**Benefits**:
- Centralized creation
- Encapsulation
- Consistent initialization

### 8. Composite Pattern

**Check**:
```scala
case class Check(
  level: CheckLevel.Value,
  description: String,
  constraints: Seq[Constraint]  // Composite of constraints
)
```

**Benefits**:
- Treat individual and groups uniformly
- Hierarchical structure
- Recursive composition

### 9. Command Pattern

**Analyzers as Commands**:
```scala
trait Analyzer[S <: State[_], +M <: Metric[_]] {
  def calculate(data: DataFrame): M  // Command execution
}
```

**Benefits**:
- Encapsulate computation
- Defer execution
- Queue/batch commands
- Undo/redo (via states)

### 10. Visitor Pattern (Implicit)

**AnalysisRunner**:

```scala
// Visits analyzers to group and optimize
private def scanShareableAnalyzers(analyzers: Seq[Analyzer[_, _]]): Seq[ScanShareableAnalyzer[_, _]]
private def groupingAnalyzers(analyzers: Seq[Analyzer[_, _]]): Seq[GroupingAnalyzer[_, _]]
private def kllAnalyzers(analyzers: Seq[Analyzer[_, _]]): Seq[KLLSketch]
```

**Benefits**:
- Separate algorithm from structure
- Add operations without modifying classes
- Type-based dispatch

### 11. Lazy Evaluation

**StateLoaders**:
```scala
trait StateLoader {
  def load[S <: State[_]](analyzer: Analyzer[S, _]): Option[S]
}
```

States are only loaded when needed.

### 12. Memoization

**MetricsRepository**:

```scala
.reuseExistingResultsForKey(resultKey)
```

Cached results avoid recomputation.

---

## Package Structure

```
com.amazon.deequ/
│
├── VerificationSuite.scala                 # Main entry point
├── VerificationRunBuilder.scala
├── VerificationResult.scala
│
├── analyzers/                              # 42+ Analyzers
│   ├── Analyzer.scala                     # Core trait
│   ├── State.scala                        # Algebraic states
│   ├── Analysis.scala
│   ├── AnalyzerContext.scala
│   ├── StateProvider.scala
│   │
│   ├── runners/
│   │   ├── AnalysisRunner.scala          # Scan sharing optimization
│   │   ├── AnalysisRunBuilder.scala
│   │   └── AnalyzerContext.scala
│   │
│   ├── applicability/
│   │   └── Applicability.scala
│   │
│   ├── catalyst/                          # Spark Catalyst integration
│   │   └── [Catalyst optimizations]
│   │
│   └── [42+ analyzer implementations]
│       ├── Size.scala
│       ├── Completeness.scala
│       ├── Uniqueness.scala
│       ├── Mean.scala
│       ├── Histogram.scala
│       ├── KLLSketch.scala
│       └── ...
│
├── checks/
│   ├── Check.scala
│   ├── CheckLevel.scala
│   ├── CheckResult.scala
│   └── CheckWithLastConstraintFilterable.scala
│
├── constraints/
│   ├── Constraint.scala
│   ├── ConstraintResult.scala
│   ├── ConstraintStatus.scala
│   ├── AnalysisBasedConstraint.scala
│   ├── ConstraintDecorator.scala
│   └── ConstrainableDataTypes.scala
│
├── anomalydetection/
│   ├── AnomalyDetectionStrategy.scala
│   ├── AnomalyDetector.scala
│   ├── Anomaly.scala
│   ├── OnlineNormalStrategy.scala
│   ├── BatchNormalStrategy.scala
│   ├── SimpleThresholdStrategy.scala
│   ├── AbsoluteChangeStrategy.scala
│   ├── RateOfChangeStrategy.scala
│   ├── RelativeRateOfChangeStrategy.scala
│   └── seasonal/
│
├── profiles/
│   ├── ColumnProfiler.scala
│   ├── ColumnProfilerRunner.scala
│   ├── ColumnProfilerRunBuilder.scala
│   ├── ColumnProfile.scala
│   ├── NumericColumnProfile.scala
│   └── StandardColumnProfile.scala
│
├── suggestions/
│   ├── ConstraintSuggestionRunner.scala
│   ├── ConstraintSuggestionRunBuilder.scala
│   ├── ConstraintSuggestion.scala
│   ├── ConstraintSuggestionResult.scala
│   └── rules/
│       ├── ConstraintRule.scala
│       ├── Rules.scala                    # Rule sets
│       ├── CompleteIfCompleteRule.scala
│       ├── RetainCompletenessRule.scala
│       ├── RetainTypeRule.scala
│       ├── CategoricalRangeRule.scala
│       ├── UniqueIfApproximatelyUniqueRule.scala
│       ├── NonNegativeNumbersRule.scala
│       ├── HasMinLength.scala
│       ├── HasMaxLength.scala
│       ├── HasMin.scala
│       ├── HasMax.scala
│       ├── HasMean.scala
│       ├── HasStandardDeviation.scala
│       └── interval/
│
├── repository/
│   ├── MetricsRepository.scala
│   ├── MetricsRepositoryMultipleResultsLoader.scala
│   ├── ResultKey.scala
│   ├── AnalysisResult.scala
│   ├── AnalysisResultSerde.scala
│   │
│   ├── fs/
│   │   └── FileSystemMetricsRepository.scala
│   │
│   ├── memory/
│   │   └── InMemoryMetricsRepository.scala
│   │
│   └── sparktable/
│       └── SparkMetricsRepository.scala
│
├── metrics/
│   ├── Metric.scala
│   ├── DoubleMetric.scala
│   ├── Entity.scala
│   ├── HistogramMetric.scala
│   ├── Distribution.scala
│   ├── KLLMetric.scala
│   └── HistogramBinnedMetric.scala
│
├── comparison/
│   ├── ComparisonBase.scala
│   ├── DataSynchronization.scala
│   ├── ReferentialIntegrity.scala
│   └── ComparisonResult.scala
│
├── dqdl/                                  # Data Quality Definition Language
│   ├── EvaluateDataQuality.scala
│   ├── model/
│   ├── translation/
│   └── execution/
│
├── schema/
│   └── RowLevelSchemaValidator.scala
│
├── io/
│   ├── DfsUtils.scala
│   └── [I/O utilities]
│
├── utilities/
│   ├── ColumnUtil.scala
│   └── [Utility classes]
│
└── examples/
    ├── BasicExample.scala
    ├── DataProfilingExample.scala
    ├── AnomalyDetectionExample.scala
    ├── ConstraintSuggestionExample.scala
    ├── IncrementalMetricsExample.scala
    ├── MetricsRepositoryExample.scala
    └── UpdateMetricsOnPartitionedDataExample.scala
```

---

## Summary

### Deequ Architecture Strengths

1. **Algebraic Design** - States as commutative semi-groups enable incremental computation
2. **Scan Sharing** - Automatic optimization reduces data scans
3. **Type Safety** - Scala's type system ensures correctness
4. **Composability** - Analyzers, checks, constraints compose elegantly
5. **Extensibility** - Easy to add analyzers, constraints, strategies
6. **Spark Integration** - Deep integration with Catalyst optimizer
7. **Multiple Storage Backends** - Flexible persistence options
8. **Profiling** - Efficient multi-pass profiling strategy
9. **Auto-Suggestions** - Generate constraints from data automatically
10. **Anomaly Detection** - Multiple time-series strategies

### Key Differentiators

- **42+ Analyzers** covering all common data quality metrics
- **60+ Constraint Methods** for comprehensive validation
- **Incremental Computation** via algebraic states
- **Partition Support** for distributed processing
- **Row-Level Results** for detailed debugging
- **Custom Analyzers** via SQL and Spark aggregations
- **Dataset Comparison** for synchronization validation
- **DQDL** for declarative rule definition
- **Automatic Optimization** through scan sharing
- **Historical Tracking** via metrics repository

### Advanced Capabilities

- Process data incrementally across time periods
- Distribute computation across partitions
- Store and query historical metrics
- Detect anomalies in time series
- Auto-generate constraint suggestions
- Compare and synchronize datasets
- Profile large datasets efficiently
- Execute custom SQL validations
- Track row-level constraint violations
- Persist and reuse computation states

---

**End of Reference Document**
