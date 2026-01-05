"""
Management command to queue all tickers from JSON file.

This command reads all stock tickers from the JSON file and
submits them one at a time to the ticker queue endpoint.
"""

import json
from pathlib import Path

import requests
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    """Queue all tickers from JSON file for processing."""

    help = "Queue all stock tickers from JSON file to the ticker queue endpoint"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tickers-file",
            type=str,
            default="stock.json",
            help="Path to the ticker symbols JSON file (default: stock.json)",
        )
        parser.add_argument(
            "--base-url",
            type=str,
            default="http://localhost:8000",
            help="Base URL of the API (default: http://localhost:8000)",
        )
        parser.add_argument(
            "--requested-by",
            type=str,
            default="test@example.com",
            help="Email address for requested_by field (default: test@example.com)",
        )
    def handle(self, *args, **options):
        """Execute the command."""
        tickers_file = options["tickers_file"]
        base_url = options["base_url"]
        requested_by = options["requested_by"]
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

        self.stdout.write(f"Found {len(stock_symbols)} stock symbols")
        self.stdout.write(f"Base URL: {base_url}")
        self.stdout.write(f"Requested by: {requested_by}\n")

        # Extract ticker symbols (remove :US suffix if present)
        tickers = [symbol.split(":")[0] for symbol in stock_symbols]
        tickers = tickers[::-1]
        
        # Send requests
        url = f"{base_url}/api/ticker/queue"
        for ticker in tickers:
            payload = {
                "ticker": ticker,
                "requested_by": requested_by,
            }
            
            try:
                requests.post(url, json=payload, timeout=300)
                self.stdout.write(f"{ticker}: sent")
            except Exception as e:
                self.stdout.write(f"{ticker}: Error - {str(e)}")
