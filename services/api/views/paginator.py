"""
Pagination helpers for the Stock Ticker ETL Pipeline API.

This module provides standard cursor-based pagination classes for API list views,
ensuring consistent and efficient ordering, page size limiting, and client-side
control of page size within safe bounds. These classes are designed for use with
Django REST Framework's generic views and are optimized for high-performance
pagination of large querysets such as stocks, runs, and bulk queue jobs.

- StandardCursorPagination: Default configuration used across API endpoints,
  ordering by creation date in descending order, with a default page size of 50
  and a maximum of 100 per page.

Best practices:
- Always use cursor-based pagination for endpoints that may deal with large tables.
- Adjust the ordering and page size as needed per API resource type.

See: services/api/views.py for usage examples.

"""
from rest_framework.pagination import CursorPagination


class StandardCursorPagination(CursorPagination):
    """Standard cursor pagination configuration for list views."""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100
    ordering = '-created_at'
