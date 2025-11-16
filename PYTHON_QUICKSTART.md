# PyDeequ Polars - Quick Start Guide

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install the package in development mode
pip install -e .
```

## Running Examples

```bash
# Run the basic example
python examples/basic_example.py

# Run the metrics repository example
python examples/metrics_repository_example.py

# Run the advanced checks example
python examples/advanced_checks_example.py
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=pydeequ_polars --cov-report=html

# Run specific test file
pytest tests/test_analyzers.py

# Run specific test class
pytest tests/test_analyzers.py::TestCompleteness

# Run specific test
pytest tests/test_analyzers.py::TestCompleteness::test_completeness_full
```

## Basic Usage

```python
import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel

# Create your DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 30, 35, 40, 45]
})

# Define and run checks
result = (
    VerificationSuite()
    .on_data(df)
    .add_check(
        Check(CheckLevel.ERROR, "Basic Data Quality")
        .has_size(lambda x: x == 5)
        .is_complete("id")
        .is_unique("id")
        .is_complete("name")
        .is_non_negative("age")
    )
    .run()
)

# Check results
print(f"Status: {result.status}")
```

## Project Structure

```
pydeequ_polars/
├── __init__.py                    # Package initialization
├── analyzers/                     # Metric computation
│   ├── __init__.py
│   ├── analyzer.py               # Base classes
│   ├── size.py                   # Row count
│   ├── completeness.py           # Non-null ratio
│   ├── uniqueness.py             # Unique value ratio
│   ├── distinctness.py           # Distinct value count
│   ├── min_max.py                # Min/Max values
│   ├── mean.py                   # Average
│   ├── sum.py                    # Sum
│   ├── standard_deviation.py    # Standard deviation
│   ├── approx_quantile.py       # Quantiles
│   ├── pattern_match.py         # Regex matching
│   └── correlation.py           # Correlation
├── constraints/                   # Constraint evaluation
│   ├── __init__.py
│   └── constraint.py
├── checks/                        # Check grouping
│   ├── __init__.py
│   └── check.py
├── repository/                    # Metrics persistence
│   ├── __init__.py
│   └── metrics_repository.py
├── utils/                         # Utilities
│   └── __init__.py
├── verification_suite.py          # Main entry point
└── verification_result.py         # Results container

tests/                             # Unit tests
├── __init__.py
├── test_analyzers.py
├── test_checks.py
├── test_verification_suite.py
└── test_repository.py

examples/                          # Examples
├── basic_example.py
├── metrics_repository_example.py
└── advanced_checks_example.py
```

## Development

```bash
# Format code
black pydeequ_polars tests examples

# Lint code
ruff check pydeequ_polars tests examples

# Type check
mypy pydeequ_polars
```

## Next Steps

1. Check out the [full README](PYTHON_README.md) for detailed documentation
2. Explore the [examples](examples/) directory
3. Read the [API documentation](PYTHON_README.md#available-analyzers)
4. Contribute to the project!

## Common Patterns

### Validating CSV Files

```python
import polars as pl
from pydeequ_polars import VerificationSuite, Check, CheckLevel

df = pl.read_csv("data.csv")

result = (
    VerificationSuite()
    .on_data(df)
    .add_check(
        Check(CheckLevel.ERROR, "CSV Validation")
        .has_size(lambda x: x > 0)
        .is_complete("required_column")
    )
    .run()
)
```

### Tracking Metrics Over Time

```python
from pydeequ_polars.repository import FileSystemMetricsRepository
from pydeequ_polars.repository.metrics_repository import MetricEntry
from pydeequ_polars.analyzers import Size, Completeness
from datetime import datetime

repo = FileSystemMetricsRepository("./metrics")

# Compute and save
analyzers = [Size(), Completeness("column")]
for analyzer in analyzers:
    metric = analyzer.calculate(df)
    entry = MetricEntry(metric, datetime.now(), {"source": "daily_batch"})
    repo.save(entry)

# Load and analyze trends
historical_size = repo.load(metric_name="Size")
for entry in historical_size:
    print(f"{entry.dataset_date}: {entry.metric.value} rows")
```

### Custom Validation Logic

```python
check = (
    Check(CheckLevel.ERROR, "Custom Validation")
    .has_mean("price", lambda x: 10 <= x <= 1000)
    .has_standard_deviation("price", lambda x: x < 100)
    .has_correlation("feature1", "feature2", lambda x: abs(x) < 0.95)
)
```
