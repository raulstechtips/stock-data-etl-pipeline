"""
Services module for the API application.

This module contains business logic services that implement
the core functionality of the Stock Ticker ETL Pipeline.
"""

from .stock_ingestion_service import StockIngestionService

__all__ = ['StockIngestionService']

