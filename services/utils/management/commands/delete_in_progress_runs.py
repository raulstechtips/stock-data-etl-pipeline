"""
Management command to delete all in_progress runs for stocks from JSON file.

This command reads all stock tickers from the JSON file, checks if each
stock exists in the database, and deletes all in_progress runs for that stock.
"""

import json
from pathlib import Path

from django.core.management.base import BaseCommand

from api.models import Stock, StockIngestionRun, IngestionState


class Command(BaseCommand):
    """Delete all in_progress runs for stocks from JSON file."""

    help = "Delete all in_progress runs for stocks from JSON file"

    def handle(self, *args, **options):
        """Execute the command."""
        # Load stocks from JSON file
        stocks_file = Path(__file__).parent / "data" / "stocks.json"
        
        if not stocks_file.exists():
            self.stdout.write(
                self.style.ERROR(f"Stocks file not found: {stocks_file}")
            )
            return

        with open(stocks_file, "r") as f:
            data = json.load(f)
            stock_symbols = data.get("data", [])

        if not stock_symbols:
            self.stdout.write(self.style.WARNING("No stocks found in JSON file"))
            return

        # Extract ticker symbols (remove :US suffix if present)
        tickers = [symbol.split(":")[0] for symbol in stock_symbols]

        self.stdout.write(f"Found {len(tickers)} stock symbols\n")

        deleted_count = 0
        not_found_count = 0
        no_runs_count = 0

        for ticker in tickers:
            # Normalize ticker to uppercase (same as Stock model does)
            ticker = ticker.strip().upper()
            
            # Check if stock exists in DB
            try:
                stock = Stock.objects.get(ticker=ticker)
            except Stock.DoesNotExist:
                not_found_count += 1
                self.stdout.write(f"{ticker}: Stock not found in DB")
                continue

            # Get all in_progress runs (not DONE or FAILED)
            terminal_states = [IngestionState.DONE, IngestionState.FAILED]
            in_progress_runs = StockIngestionRun.objects.filter(
                stock=stock
            ).exclude(state__in=terminal_states)

            run_count = in_progress_runs.count()
            
            if run_count == 0:
                no_runs_count += 1
                self.stdout.write(f"{ticker}: No in_progress runs found")
            else:
                # Delete all in_progress runs
                in_progress_runs.delete()
                deleted_count += run_count
                self.stdout.write(
                    f"{ticker}: Deleted {run_count} in_progress run(s)"
                )

        # Summary
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("Summary:")
        self.stdout.write(f"  Stocks processed: {len(tickers)}")
        self.stdout.write(f"  Stocks not found in DB: {not_found_count}")
        self.stdout.write(f"  Stocks with no in_progress runs: {no_runs_count}")
        self.stdout.write(f"  Total runs deleted: {deleted_count}")

