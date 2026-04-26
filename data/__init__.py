"""Data loading module."""

from .duckdb.load_csv import load_csv_to_duckdb

__all__ = ["load_csv_to_duckdb"]
