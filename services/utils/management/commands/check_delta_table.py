"""
Management command to check and display the unified Delta Lake stocks table for a ticker.

This command retrieves data from the unified stocks Delta Lake table and filters
by ticker and record_type (financials, TTM, metadata) for inspection and testing.

Usage:
    python manage.py check_delta_table TICKERONE
    python manage.py check_delta_table TICKERONE --record-type financials
    python manage.py check_delta_table TICKERONE --limit 5
    python manage.py check_delta_table TICKERONE --test-mode  # Show only test JSON columns
"""

import sys
from urllib.parse import urlparse

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    """Check and display the unified Delta Lake stocks table for a ticker."""

    help = "Retrieve and display unified stocks table data for a given ticker"
    
    # Test mode columns based on TICKERONE_v2.json structure
    TEST_COLUMNS = {
        "financials": [
            "ticker",
            "record_type",
            "period_end_date",
            "revenue",
            "fiscal_quarter_key",
            "fiscal_quarter_number",
        ],
        "ttm": [
            "ticker",
            "record_type",
            "period_end_date",
            "revenue",
            "net_income",
        ],
        "metadata": [
            "ticker",
            "record_type",
            "period_end_date",  # Will be null for metadata
            "name",
            "symbol",
        ],
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "ticker",
            type=str,
            help="Stock ticker symbol (e.g., TICKERONE, AAPL, MSFT)",
        )
        parser.add_argument(
            "--record-type",
            type=str,
            choices=["financials", "metadata", "ttm", "all"],
            default="all",
            help="Which record type to display (default: all)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit the number of rows displayed for time-series data",
        )
        parser.add_argument(
            "--sort-desc",
            action="store_true",
            help="Sort time-series data by date descending (most recent first)",
        )
        parser.add_argument(
            "--test-mode",
            action="store_true",
            help="Show only columns from test JSON for quick validation",
        )

    def handle(self, *args, **options):
        """Execute the command."""
        ticker = options["ticker"].strip().upper()
        record_type = options["record_type"]
        limit = options["limit"]
        sort_desc = options["sort_desc"]
        test_mode = options["test_mode"]

        self.stdout.write(self.style.SUCCESS(f"\n{'='*80}"))
        self.stdout.write(self.style.SUCCESS(f"  Unified Stocks Table - Data for {ticker}"))
        self.stdout.write(self.style.SUCCESS(f"{'='*80}\n"))

        # Build storage options for Delta Lake
        storage_options = self._build_storage_options()

        # Display requested record types
        if record_type == "all":
            self._display_record_type(ticker, "financials", storage_options, limit, sort_desc, test_mode)
            self._display_record_type(ticker, "ttm", storage_options, limit, sort_desc, test_mode)
            self._display_record_type(ticker, "metadata", storage_options, None, False, test_mode)
        else:
            self._display_record_type(ticker, record_type, storage_options, limit, sort_desc, test_mode)

        self.stdout.write(self.style.SUCCESS(f"\n{'='*80}\n"))

    def _build_storage_options(self):
        """
        Build storage options dictionary for Delta Lake S3 access.

        Returns:
            Dict with AWS credentials and endpoint configuration
        """
        parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
        endpoint = parsed.netloc or parsed.path

        storage_options = {
            'AWS_ACCESS_KEY_ID': settings.AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': settings.AWS_SECRET_ACCESS_KEY,
            'AWS_ENDPOINT_URL': settings.AWS_S3_ENDPOINT_URL,
            'AWS_REGION': settings.AWS_S3_REGION_NAME or 'us-east-1',
            'AWS_ALLOW_HTTP': 'true',
            'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
            "conditional_put": "etag",
        }

        return storage_options

    def _display_record_type(self, ticker, record_type, storage_options, limit, sort_desc, test_mode):
        """
        Display data for a specific record type from the unified stocks table.

        Args:
            ticker: Stock ticker symbol
            record_type: Type of data (financials, metadata, ttm)
            storage_options: S3 storage options
            limit: Maximum number of rows to display (None for all)
            sort_desc: Sort time-series data descending by date
            test_mode: If True, show only columns from test JSON
        """
        table_path = f"s3://{settings.STOCK_DELTA_LAKE_BUCKET}/stocks"

        self.stdout.write(self.style.HTTP_INFO(f"\n{'-'*80}"))
        self.stdout.write(self.style.HTTP_INFO(f"  {record_type.upper()} DATA (record_type='{record_type}')"))
        self.stdout.write(self.style.HTTP_INFO(f"{'-'*80}"))
        self.stdout.write(f"Table Path: {table_path}\n")
        self.stdout.write(f"Filter: ticker='{ticker}' AND record_type='{record_type}'\n")

        try:
            # Load unified Delta Lake stocks table
            dt = DeltaTable(table_path, storage_options=storage_options)
            
            # Convert to Polars DataFrame
            df = dt.to_pyarrow_table()
            df = pl.from_arrow(df)

            if df.is_empty():
                self.stdout.write(self.style.WARNING("  (Unified stocks table is empty)\n"))
                return

            # Filter by ticker and record_type
            filtered_df = df.filter(
                (pl.col("ticker") == ticker) & 
                (pl.col("record_type") == record_type)
            )

            if filtered_df.is_empty():
                self.stdout.write(
                    self.style.WARNING(
                        f"  No data found for ticker='{ticker}' and record_type='{record_type}'\n"
                    )
                )
                return

            # Sort time-series data by period_end_date if available
            if "period_end_date" in filtered_df.columns:
                if sort_desc:
                    filtered_df = filtered_df.sort("period_end_date", descending=True)
                else:
                    filtered_df = filtered_df.sort("period_end_date", descending=False)

            # Apply limit if specified
            if limit is not None and record_type in ["financials", "ttm"]:
                filtered_df = filtered_df.head(limit)

            # Display summary
            self.stdout.write(f"Rows: {len(filtered_df)}")
            self.stdout.write(f"Columns: {len(filtered_df.columns)}\n")

            # Display data based on record type
            if record_type == "financials":
                self._display_financials(filtered_df, test_mode)
            elif record_type == "ttm":
                self._display_ttm(filtered_df, test_mode)
            elif record_type == "metadata":
                self._display_metadata(filtered_df, test_mode)

        except TableNotFoundError:
            self.stdout.write(
                self.style.WARNING(f"  Unified stocks table not found: {table_path}")
            )
            self.stdout.write(
                self.style.WARNING(
                    f"  (The table may not have been created yet)\n"
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"  Error reading table: {str(e)}\n")
            )
            raise CommandError(f"Failed to read Delta Lake table: {str(e)}")

    def _display_financials(self, df, test_mode=False):
        """
        Display financial time-series data in a readable format.

        Args:
            df: Polars DataFrame with financial data
            test_mode: If True, show only test JSON columns
        """
        self.stdout.write("\n" + self.style.SUCCESS("Financial Time Series Data:"))
        self.stdout.write("(Showing quarterly financial metrics over time)\n")
        
        if test_mode:
            self.stdout.write(self.style.WARNING("  TEST MODE: Showing only test JSON columns\n"))

        # Determine columns to display
        if test_mode:
            # Use only test columns that exist in the DataFrame
            key_columns = [col for col in self.TEST_COLUMNS["financials"] if col in df.columns]
        else:
            # Key columns to display prominently in normal mode
            key_columns = [
                "ticker",
                "record_type",
                "period_end_date",
                "revenue",
                "net_income",
                "total_assets",
                "total_liabilities",
                "shareholders_equity",
                "operating_cash_flow",
                "free_cash_flow",
            ]

        # Filter to columns that exist in the DataFrame
        display_columns = [col for col in key_columns if col in df.columns]

        if display_columns:
            # Display the key metrics using Polars' native string representation
            with pl.Config(
                tbl_rows=-1,  # Show all rows
                fmt_str_lengths=50,  # Truncate long strings
                tbl_width_chars=200,  # Wide table for terminal
            ):
                self.stdout.write(str(df.select(display_columns)))
        else:
            # Fallback: display all columns
            with pl.Config(tbl_rows=-1, fmt_str_lengths=50, tbl_width_chars=200):
                self.stdout.write(str(df))

        # Show column summary
        all_columns = df.columns
        if len(all_columns) > len(display_columns):
            other_columns = [col for col in all_columns if col not in display_columns]
            self.stdout.write(
                f"\n\nAdditional columns available ({len(other_columns)}): "
                + ", ".join(other_columns[:10])
            )
            if len(other_columns) > 10:
                self.stdout.write(f" ... and {len(other_columns) - 10} more")

        self.stdout.write("\n")

    def _display_ttm(self, df, test_mode=False):
        """
        Display TTM (Trailing Twelve Month) data.

        Args:
            df: Polars DataFrame with TTM data
            test_mode: If True, show only test JSON columns
        """
        self.stdout.write("\n" + self.style.SUCCESS("TTM (Trailing Twelve Month) Data:"))
        self.stdout.write("(Showing 12-month rolling financial metrics)\n")
        
        if test_mode:
            self.stdout.write(self.style.WARNING("  TEST MODE: Showing only test JSON columns\n"))

        # Determine columns to display
        if test_mode:
            # Use only test columns that exist in the DataFrame
            key_columns = [col for col in self.TEST_COLUMNS["ttm"] if col in df.columns]
        else:
            # Key columns to display prominently in normal mode
            key_columns = [
                "ticker",
                "record_type",
                "period_end_date",
                "revenue",
                "net_income",
                "operating_cash_flow",
                "free_cash_flow",
                "earnings_per_share_basic",
                "book_value_per_share",
            ]

        # Filter to columns that exist in the DataFrame
        display_columns = [col for col in key_columns if col in df.columns]

        if display_columns:
            # Display the key metrics using Polars' native string representation
            with pl.Config(
                tbl_rows=-1,  # Show all rows
                fmt_str_lengths=50,  # Truncate long strings
                tbl_width_chars=200,  # Wide table for terminal
            ):
                self.stdout.write(str(df.select(display_columns)))
        else:
            # Fallback: display all columns
            with pl.Config(tbl_rows=-1, fmt_str_lengths=50, tbl_width_chars=200):
                self.stdout.write(str(df))

        # Show column summary
        all_columns = df.columns
        if len(all_columns) > len(display_columns):
            other_columns = [col for col in all_columns if col not in display_columns]
            self.stdout.write(
                f"\n\nAdditional columns available ({len(other_columns)}): "
                + ", ".join(other_columns[:10])
            )
            if len(other_columns) > 10:
                self.stdout.write(f" ... and {len(other_columns) - 10} more")

        self.stdout.write("\n")

    def _display_metadata(self, df, test_mode=False):
        """
        Display company metadata in a readable format.

        Args:
            df: Polars DataFrame with metadata
            test_mode: If True, show only test JSON columns
        """
        self.stdout.write("\n" + self.style.SUCCESS("Company Metadata:"))
        self.stdout.write("(Non-time-series company information)\n")
        
        if test_mode:
            self.stdout.write(self.style.WARNING("  TEST MODE: Showing only test JSON columns\n"))

        # Filter columns if in test mode
        if test_mode:
            # Use only test columns that exist in the DataFrame
            test_cols = [col for col in self.TEST_COLUMNS["metadata"] if col in df.columns]
            if test_cols:
                df = df.select(test_cols)

        # Convert to dict for better display
        if len(df) > 0:
            record = df.to_dicts()[0]
            
            # Display in key: value format
            for key, value in record.items():
                # Format value for display
                if value is None:
                    value_str = "(null)"
                elif isinstance(value, float):
                    value_str = f"{value:,.2f}" if abs(value) < 1e10 else f"{value:.2e}"
                else:
                    value_str = str(value)
                
                # Indent and display
                self.stdout.write(f"  {key:30s}: {value_str}")
        
        # Show additional columns available in normal mode
        if not test_mode and len(df) > 0:
            all_columns = df.columns
            display_columns = list(record.keys())
            if len(all_columns) > len(display_columns):
                other_columns = [col for col in all_columns if col not in display_columns]
                self.stdout.write(
                    f"\n\nAdditional columns available: "
                    + ", ".join(other_columns)
                )

        self.stdout.write("\n")
