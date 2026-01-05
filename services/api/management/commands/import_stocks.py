"""
Management command to import stocks from JSON file into database.

This command:
1. Reads all stock tickers from the JSON file
2. Creates Stock model instances in the database (get_or_create)

"""

import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction

from api.models import Stock


class Command(BaseCommand):
    """Import stocks from JSON file into database."""

    help = "Import stock tickers from JSON file into database and queue them for ingestion"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tickers-file",
            type=str,
            default="US_TICKERS.json",
            help="Ticker symbols JSON file to import US_TICKERS.json or US_OTC_TICKERS.json (default: US_TICKERS.json)",
        )

    def handle(self, *args, **options):
        """Execute the command."""
        tickers_file = options["tickers_file"]

        # Load stocks from JSON file
        stocks_file = Path(__file__).parent / "data" / f"{tickers_file}"

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

        self.stdout.write(f"Found {len(stock_symbols)} stock symbols in file")

        # Extract ticker symbols (remove :US suffix if present)
        tickers = [symbol.split(":")[0].strip().upper() for symbol in stock_symbols]

        # Step 1: Create stocks in database using get_or_create
        self.stdout.write("Creating stocks in database...")
        created_count = 0
        existing_count = 0
        error_count = 0

        # Use bulk operations for better performance
        # Process in batches to avoid memory issues with large files
        batch_size = 1000
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            with transaction.atomic():
                for ticker in batch:
                    try:
                        stock, created = Stock.objects.get_or_create(ticker=ticker)
                        if created:
                            created_count += 1
                        else:
                            existing_count += 1
                    except Exception as e:
                        error_count += 1
                        self.stdout.write(
                            self.style.WARNING(f"Error creating {ticker}: {str(e)}")
                        )

            # Progress update
            processed = min(i + batch_size, len(tickers))
            self.stdout.write(
                f"Processed {processed}/{len(tickers)} tickers "
                f"(Created: {created_count}, Existing: {existing_count}, Errors: {error_count})"
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nStock import complete:\n"
                f"  Created: {created_count}\n"
                f"  Existing: {existing_count}\n"
                f"  Errors: {error_count}\n"
            )
        )
