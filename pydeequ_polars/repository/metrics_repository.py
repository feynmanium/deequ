"""Metrics repository for storing and retrieving computed metrics."""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import os
from pathlib import Path

from pydeequ_polars.analyzers.analyzer import Metric


class MetricEntry:
    """Represents a stored metric with metadata."""

    def __init__(
        self,
        metric: Metric,
        dataset_date: datetime,
        tags: Optional[Dict[str, str]] = None
    ):
        self.metric = metric
        self.dataset_date = dataset_date
        self.tags = tags or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "metric_name": self.metric.name,
            "metric_value": self.metric.value,
            "metric_entity": self.metric.entity,
            "dataset_date": self.dataset_date.isoformat(),
            "tags": self.tags
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetricEntry":
        """Create from dictionary."""
        metric = Metric(
            name=data["metric_name"],
            value=data["metric_value"],
            entity=data["metric_entity"]
        )
        dataset_date = datetime.fromisoformat(data["dataset_date"])
        tags = data.get("tags", {})
        return cls(metric, dataset_date, tags)


class MetricsRepository(ABC):
    """Abstract base class for metrics repositories."""

    @abstractmethod
    def save(self, entry: MetricEntry) -> None:
        """Save a metric entry."""
        pass

    @abstractmethod
    def load(
        self,
        metric_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> List[MetricEntry]:
        """Load metric entries, optionally filtered by name and tags."""
        pass


class InMemoryMetricsRepository(MetricsRepository):
    """In-memory implementation of metrics repository."""

    def __init__(self):
        self._entries: List[MetricEntry] = []

    def save(self, entry: MetricEntry) -> None:
        """Save a metric entry to memory."""
        self._entries.append(entry)

    def load(
        self,
        metric_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> List[MetricEntry]:
        """Load metric entries from memory."""
        results = self._entries

        if metric_name:
            results = [e for e in results if e.metric.name == metric_name]

        if tags:
            results = [
                e for e in results
                if all(e.tags.get(k) == v for k, v in tags.items())
            ]

        return results


class FileSystemMetricsRepository(MetricsRepository):
    """File system implementation of metrics repository using JSON."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, entry: MetricEntry) -> Path:
        """Generate a unique file path for an entry."""
        timestamp = entry.dataset_date.strftime("%Y%m%d_%H%M%S")
        filename = f"{entry.metric.name}_{timestamp}.json"
        return self.path / filename

    def save(self, entry: MetricEntry) -> None:
        """Save a metric entry to file system."""
        file_path = self._get_file_path(entry)
        with open(file_path, 'w') as f:
            json.dump(entry.to_dict(), f, indent=2)

    def load(
        self,
        metric_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> List[MetricEntry]:
        """Load metric entries from file system."""
        entries = []

        for file_path in self.path.glob("*.json"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    entry = MetricEntry.from_dict(data)

                    # Apply filters
                    if metric_name and entry.metric.name != metric_name:
                        continue

                    if tags and not all(entry.tags.get(k) == v for k, v in tags.items()):
                        continue

                    entries.append(entry)
            except Exception as e:
                # Skip files that can't be loaded
                print(f"Warning: Could not load {file_path}: {e}")
                continue

        return entries
