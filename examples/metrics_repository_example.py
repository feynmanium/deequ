"""
Example demonstrating metrics repository functionality.

This example shows how to persist and query computed metrics over time.
"""

import polars as pl
from datetime import datetime, timedelta
from pydeequ_polars.repository import InMemoryMetricsRepository, FileSystemMetricsRepository
from pydeequ_polars.repository.metrics_repository import MetricEntry
from pydeequ_polars.analyzers import Size, Completeness, Mean


def main():
    # Create sample datasets for different time periods
    datasets = [
        (
            datetime.now() - timedelta(days=2),
            pl.DataFrame({
                "id": [1, 2, 3, 4],
                "value": [10, 20, None, 40]
            }),
            {"environment": "production", "region": "us-east"}
        ),
        (
            datetime.now() - timedelta(days=1),
            pl.DataFrame({
                "id": [1, 2, 3, 4, 5],
                "value": [15, 25, 35, None, 55]
            }),
            {"environment": "production", "region": "us-east"}
        ),
        (
            datetime.now(),
            pl.DataFrame({
                "id": [1, 2, 3, 4, 5, 6],
                "value": [10, 20, 30, 40, 50, 60]
            }),
            {"environment": "production", "region": "us-east"}
        ),
    ]

    # Use in-memory repository
    print("Using InMemoryMetricsRepository")
    print("="*80)
    repository = InMemoryMetricsRepository()

    # Compute and save metrics for each dataset
    analyzers = [
        Size(),
        Completeness("value"),
        Mean("value")
    ]

    for dataset_date, df, tags in datasets:
        print(f"\nProcessing data from {dataset_date.strftime('%Y-%m-%d')}")

        for analyzer in analyzers:
            metric = analyzer.calculate(df)
            entry = MetricEntry(metric, dataset_date, tags)
            repository.save(entry)

            print(f"  Saved: {metric.name} = {metric.value}")

    # Query metrics
    print("\n" + "="*80)
    print("Querying all Size metrics:")

    size_metrics = repository.load(metric_name="Size")
    for entry in size_metrics:
        print(f"  Date: {entry.dataset_date.strftime('%Y-%m-%d')}, "
              f"Value: {entry.metric.value}, "
              f"Tags: {entry.tags}")

    print("\n" + "="*80)
    print("Querying all Completeness metrics:")

    completeness_metrics = repository.load(metric_name="Completeness(value)")
    for entry in completeness_metrics:
        print(f"  Date: {entry.dataset_date.strftime('%Y-%m-%d')}, "
              f"Value: {entry.metric.value:.2%}")

    print("\n" + "="*80)
    print("Querying metrics with specific tags:")

    tagged_metrics = repository.load(tags={"environment": "production", "region": "us-east"})
    print(f"Found {len(tagged_metrics)} metrics matching the tags")

    # Demonstrate filesystem repository
    print("\n" + "="*80)
    print("Using FileSystemMetricsRepository")
    print("="*80)

    fs_repository = FileSystemMetricsRepository("/tmp/pydeequ_metrics")

    # Save one dataset to filesystem
    dataset_date, df, tags = datasets[-1]
    print(f"\nSaving metrics from {dataset_date.strftime('%Y-%m-%d')} to filesystem")

    for analyzer in analyzers:
        metric = analyzer.calculate(df)
        entry = MetricEntry(metric, dataset_date, tags)
        fs_repository.save(entry)
        print(f"  Saved to file: {metric.name} = {metric.value}")

    # Load from filesystem
    print("\nLoading metrics from filesystem:")
    loaded_metrics = fs_repository.load()
    for entry in loaded_metrics:
        print(f"  {entry.metric.name}: {entry.metric.value}")


if __name__ == "__main__":
    main()
