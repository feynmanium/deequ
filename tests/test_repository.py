"""Tests for metrics repository."""

import pytest
import polars as pl
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from pydeequ_polars.repository import InMemoryMetricsRepository, FileSystemMetricsRepository
from pydeequ_polars.repository.metrics_repository import MetricEntry
from pydeequ_polars.analyzers import Size, Completeness, Mean


class TestInMemoryRepository:
    def test_save_and_load(self):
        repo = InMemoryMetricsRepository()
        df = pl.DataFrame({"a": [1, 2, 3]})

        analyzer = Size()
        metric = analyzer.calculate(df)
        entry = MetricEntry(metric, datetime.now(), {"env": "test"})

        repo.save(entry)
        loaded = repo.load()

        assert len(loaded) == 1
        assert loaded[0].metric.value == 3

    def test_load_by_metric_name(self):
        repo = InMemoryMetricsRepository()
        df = pl.DataFrame({"a": [1, 2, 3]})

        # Save multiple metrics
        size_metric = Size().calculate(df)
        completeness_metric = Completeness("a").calculate(df)

        repo.save(MetricEntry(size_metric, datetime.now()))
        repo.save(MetricEntry(completeness_metric, datetime.now()))

        # Load only size metrics
        size_entries = repo.load(metric_name="Size")
        assert len(size_entries) == 1
        assert size_entries[0].metric.name == "Size"

    def test_load_by_tags(self):
        repo = InMemoryMetricsRepository()
        df = pl.DataFrame({"a": [1, 2, 3]})

        metric = Size().calculate(df)

        repo.save(MetricEntry(metric, datetime.now(), {"env": "prod"}))
        repo.save(MetricEntry(metric, datetime.now(), {"env": "dev"}))

        prod_entries = repo.load(tags={"env": "prod"})
        assert len(prod_entries) == 1

    def test_time_series(self):
        repo = InMemoryMetricsRepository()
        df1 = pl.DataFrame({"a": [1, 2, 3]})
        df2 = pl.DataFrame({"a": [1, 2, 3, 4, 5]})

        date1 = datetime.now() - timedelta(days=1)
        date2 = datetime.now()

        analyzer = Size()
        repo.save(MetricEntry(analyzer.calculate(df1), date1))
        repo.save(MetricEntry(analyzer.calculate(df2), date2))

        entries = repo.load(metric_name="Size")
        assert len(entries) == 2
        values = [e.metric.value for e in entries]
        assert 3 in values and 5 in values


class TestFileSystemRepository:
    def test_save_and_load(self):
        with TemporaryDirectory() as tmpdir:
            repo = FileSystemMetricsRepository(tmpdir)
            df = pl.DataFrame({"a": [1, 2, 3]})

            analyzer = Size()
            metric = analyzer.calculate(df)
            entry = MetricEntry(metric, datetime.now(), {"env": "test"})

            repo.save(entry)
            loaded = repo.load()

            assert len(loaded) == 1
            assert loaded[0].metric.value == 3

    def test_persistence(self):
        with TemporaryDirectory() as tmpdir:
            # Save with first instance
            repo1 = FileSystemMetricsRepository(tmpdir)
            df = pl.DataFrame({"a": [1, 2, 3]})
            metric = Size().calculate(df)
            entry = MetricEntry(metric, datetime.now())
            repo1.save(entry)

            # Load with second instance
            repo2 = FileSystemMetricsRepository(tmpdir)
            loaded = repo2.load()

            assert len(loaded) == 1
            assert loaded[0].metric.value == 3

    def test_multiple_metrics(self):
        with TemporaryDirectory() as tmpdir:
            repo = FileSystemMetricsRepository(tmpdir)
            df = pl.DataFrame({"a": [1, 2, 3]})

            analyzers = [Size(), Completeness("a"), Mean("a")]
            for analyzer in analyzers:
                metric = analyzer.calculate(df)
                entry = MetricEntry(metric, datetime.now())
                repo.save(entry)

            loaded = repo.load()
            assert len(loaded) == 3
