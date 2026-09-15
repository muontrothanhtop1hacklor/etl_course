"""Base pipeline abstraction for future ETL/ELT stages.

Mục đích: chuẩn bị interface để mở rộng sang Bronze/Silver/Gold,
nhất là khi cần tổng hợp nhiều job hoặc chạy batch lớn.
"""

from abc import ABC, abstractmethod


class BasePipeline(ABC):
    @abstractmethod
    def run(self):
        """Execute the pipeline for a given stage."""
        raise NotImplementedError
