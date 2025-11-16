# PyDeequ Polars - Data Quality Library for Python

A pure Python implementation of Deequ using Polars DataFrames for fast, memory-efficient data quality validation.

## Overview

PyDeequ Polars is a library for defining "unit tests for data" that measure data quality in datasets of any size. Unlike PyDeequ (which wraps the Scala Deequ library and requires PySpark), this implementation is written entirely in Python and uses Polars for high-performance data processing.

## Features

- **Pure Python**: No JVM or Spark required
- **Fast**: Leverages Polars for efficient data processing
- **Type-safe**: Full type hints throughout
- **Easy to use**: Fluent API similar to the original Deequ
- **Comprehensive**: Supports all major data quality checks
- **Extensible**: Easy to add custom analyzers and constraints

## Installation

```bash
pip install polars
```

Then add the `pydeequ_polars` package to your project.

## Quick Start

```python
import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel

# Create your DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", None, "David", "Eve"],
    "age": [25, 30, 35, 40, 45],
    "email": ["alice@example.com", "bob@test.com", "charlie@test.com",
              "dave@example.com", "eve@test.com"]
})

# Define and run checks
result = (
    VerificationSuite()
    .on_data(df)
    .add_check(
        Check(CheckLevel.ERROR, "Data Quality Check")
        .has_size(lambda x: x == 5)
        .is_complete("id")
        .is_unique("id")
        .has_completeness("name", lambda x: x >= 0.8)
        .is_non_negative("age")
        .contains_email("email", lambda x: x >= 0.9)
    )
    .run()
)

# Check results
if result.status == CheckStatus.SUCCESS:
    print("All checks passed!")
else:
    print("Some checks failed:")
    for check, check_result in result.check_results.items():
        for constraint_result in check_result.constraint_results:
            if constraint_result.status != ConstraintStatus.SUCCESS:
                print(f"  {constraint_result.constraint.name}: {constraint_result.message}")
```

## Available Analyzers

PyDeequ Polars provides the following analyzers for computing metrics:

### Basic Metrics
- **Size**: Count total number of rows
- **Completeness**: Measure ratio of non-null values
- **Uniqueness**: Measure ratio of unique values
- **Distinctness**: Count distinct values

### Statistical Metrics
- **Mean**: Calculate average value
- **Sum**: Calculate sum of values
- **Minimum**: Find minimum value
- **Maximum**: Find maximum value
- **StandardDeviation**: Calculate standard deviation
- **ApproxQuantile**: Calculate approximate quantiles

### Pattern Matching
- **PatternMatch**: Match values against regex patterns
- **Correlation**: Calculate correlation between columns

## Available Constraints

Constraints can be added to checks using a fluent API:

### Size Constraints
```python
check.has_size(lambda x: x > 100)  # At least 100 rows
check.has_size(lambda x: x == 1000)  # Exactly 1000 rows
```

### Completeness Constraints
```python
check.is_complete("column")  # No null values
check.has_completeness("column", lambda x: x >= 0.95)  # At least 95% complete
```

### Uniqueness Constraints
```python
check.is_unique("id")  # All values unique
check.has_uniqueness("email", lambda x: x >= 0.9)  # At least 90% unique
```

### Range Constraints
```python
check.is_non_negative("age")  # No negative values
check.is_positive("price")  # Only positive values
check.has_min("age", lambda x: x >= 0)
check.has_max("age", lambda x: x <= 120)
```

### Statistical Constraints
```python
check.has_mean("value", lambda x: 10 <= x <= 100)
check.has_sum("total", lambda x: x == 1000)
check.has_standard_deviation("value", lambda x: x < 10)
check.has_approx_quantile("value", 0.5, lambda x: x <= 100)  # Median
```

### Pattern Constraints
```python
check.contains_url("url_column", lambda x: x >= 0.9)
check.contains_email("email_column", lambda x: x >= 0.95)
check.matches_pattern("phone", r"^\d{3}-\d{3}-\d{4}$", lambda x: x >= 0.8)
```

### Categorical Constraints
```python
check.is_contained_in("status", ["active", "inactive", "pending"])
```

### Correlation Constraints
```python
check.has_correlation("feature1", "feature2", lambda x: abs(x) < 0.9)
```

## Metrics Repository

Store and retrieve metrics over time for tracking data quality trends:

```python
from pydeequ_polars.repository import InMemoryMetricsRepository, FileSystemMetricsRepository
from pydeequ_polars.repository.metrics_repository import MetricEntry
from pydeequ_polars.analyzers import Size, Completeness, Mean
from datetime import datetime

# Create repository
repository = InMemoryMetricsRepository()
# Or use filesystem: repository = FileSystemMetricsRepository("/path/to/metrics")

# Compute and save metrics
analyzers = [Size(), Completeness("column"), Mean("value")]
for analyzer in analyzers:
    metric = analyzer.calculate(df)
    entry = MetricEntry(
        metric=metric,
        dataset_date=datetime.now(),
        tags={"environment": "production", "source": "api"}
    )
    repository.save(entry)

# Query metrics
size_metrics = repository.load(metric_name="Size")
prod_metrics = repository.load(tags={"environment": "production"})
```

## Check Levels

Checks can have different severity levels:

- **CheckLevel.ERROR**: Critical checks that must pass
- **CheckLevel.WARNING**: Important checks that should pass but won't fail the verification

```python
.add_check(
    Check(CheckLevel.ERROR, "Critical validations")
    .is_complete("id")
    .is_unique("id")
)
.add_check(
    Check(CheckLevel.WARNING, "Best practices")
    .has_completeness("optional_field", lambda x: x >= 0.8)
)
```

## Examples

See the `examples/` directory for complete working examples:

- `basic_example.py`: Basic usage mirroring the original Deequ example
- `metrics_repository_example.py`: Storing and querying metrics over time
- `advanced_checks_example.py`: Complex validation scenarios with multiple checks

## Architecture

PyDeequ Polars follows a clean architecture:

```
pydeequ_polars/
├── analyzers/          # Metric computation
│   ├── analyzer.py     # Base analyzer and Metric classes
│   ├── size.py
│   ├── completeness.py
│   └── ...
├── constraints/        # Constraint evaluation
│   └── constraint.py
├── checks/            # Check grouping
│   └── check.py
├── repository/        # Metrics persistence
│   └── metrics_repository.py
├── verification_suite.py  # Main entry point
└── verification_result.py # Results container
```

## Comparison with Original Deequ

| Feature | Original Deequ | PyDeequ Polars |
|---------|---------------|----------------|
| Language | Scala | Python |
| Runtime | JVM (Spark) | Native Python |
| DataFrame | Spark DataFrame | Polars DataFrame |
| Performance | Distributed (Spark) | In-memory (Polars) |
| Dependencies | Scala, Spark, Java | Python, Polars |
| Use Case | Big Data (billions of rows) | Medium Data (millions of rows) |

## Performance

Polars is extremely fast for in-memory operations. For datasets that fit in memory (up to millions of rows), PyDeequ Polars will typically be faster than Spark-based solutions due to:

- No JVM overhead
- Optimized in-memory processing
- Parallel execution using all CPU cores
- Zero serialization overhead

## Contributing

Contributions are welcome! The codebase is organized for easy extension:

1. Add new analyzers in `analyzers/`
2. Add new constraint types in `checks/check.py`
3. Add examples in `examples/`
4. Add tests in `tests/`

## License

Apache 2.0 License (same as original Deequ)

## Credits

This implementation is inspired by AWS Deequ and follows similar design patterns and API conventions.
