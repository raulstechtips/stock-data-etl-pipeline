"""
Management command to assign exchange to stocks from JSON file.

This command:
1. Validates that the specified exchange exists in the database
2. Reads all stock tickers from the JSON file
3. Batch updates stocks to assign the exchange

"""

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Stock, Exchange


class Command(BaseCommand):
    """Assign exchange to stocks from JSON file."""

    help = "Assign an exchange to stock tickers from JSON file"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tickers-file",
            type=str,
            default="US_TICKERS.json",
            help="Ticker symbols JSON file to import US_TICKERS.json or US_OTC_TICKERS.json (default: US_TICKERS.json)",
        )
        parser.add_argument(
            "--exchange",
            type=str,
            required=True,
            help="Exchange name (e.g., NASDAQ, NYSE). Must exist in the database.",
        )

    def handle(self, *args, **options):
        """Execute the command."""
        tickers_file = options["tickers_file"]
        exchange_name = options["exchange"]

        # Step 1: Validate exchange exists
        self.stdout.write(f"Looking up exchange: {exchange_name}")
        try:
            # Exchange.save() normalizes to uppercase, so we normalize here too for lookup
            exchange = Exchange.objects.get(name=exchange_name.strip().upper())
            self.stdout.write(
                self.style.SUCCESS(f"Found exchange: {exchange.name} (ID: {exchange.id})")
            )
        except Exchange.DoesNotExist:
            raise CommandError(
                f"Exchange '{exchange_name}' does not exist in the database. "
                f"Please create it first or check the name."
            )

        # Step 2: Load stocks from JSON file
        stocks_file = Path(__file__).parent / "data" / f"{tickers_file}"

        if not stocks_file.exists():
            raise CommandError(f"Stocks file not found: {stocks_file}")

        with open(stocks_file, "r") as f:
            data = json.load(f)
            stock_symbols = data.get("data", [])

        if not stock_symbols:
            self.stdout.write(self.style.WARNING("No stocks found in JSON file"))
            return

        self.stdout.write(f"Found {len(stock_symbols)} stock symbols in file")

        # Step 3: Extract ticker symbols (remove :US suffix if present)
        tickers = [symbol.split(":")[0].strip().upper() for symbol in stock_symbols]

        # Step 4: Batch assign exchange to stocks using bulk_update
        self.stdout.write(f"Assigning exchange '{exchange.name}' to stocks...")
        updated_count = 0
        not_found_count = 0
        error_count = 0

        # Process in batches to avoid memory issues with large files
        batch_size = 1000
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            
            with transaction.atomic():
                try:
                    # Get all stocks in this batch that exist in the database
                    stocks_in_batch = Stock.objects.filter(ticker__in=batch)
                    
                    # Track which stocks were found
                    found_tickers = set()
                    stocks_to_update = []
                    
                    for stock in stocks_in_batch:
                        stock.exchange = exchange
                        stocks_to_update.append(stock)
                        found_tickers.add(stock.ticker)
                    
                    # Perform bulk update - much more efficient than individual saves
                    if stocks_to_update:
                        Stock.objects.bulk_update(
                            stocks_to_update, 
                            ['exchange'], 
                            batch_size=batch_size
                        )
                        updated_count += len(stocks_to_update)
                    
                    # Track stocks not found in database
                    for ticker in batch:
                        if ticker not in found_tickers:
                            not_found_count += 1
                            if not_found_count <= 10:  # Only show first 10 warnings
                                self.stdout.write(
                                    self.style.WARNING(f"Stock {ticker} not found in database")
                                )
                
                except Exception as e:
                    error_count += len(batch)
                    self.stdout.write(
                        self.style.ERROR(f"Error updating batch: {str(e)}")
                    )

            # Progress update
            processed = min(i + batch_size, len(tickers))
            self.stdout.write(
                f"Processed {processed}/{len(tickers)} tickers "
                f"(Updated: {updated_count}, Not Found: {not_found_count}, Errors: {error_count})"
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nExchange assignment complete:\n"
                f"  Exchange: {exchange.name}\n"
                f"  Updated: {updated_count}\n"
                f"  Not Found: {not_found_count}\n"
                f"  Errors: {error_count}\n"
            )
        )
