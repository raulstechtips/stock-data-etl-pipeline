"""
API Stock Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- GET /ticker/<ticker>/detail - Get stock details for a specific stock
- GET /ticker/<ticker>/status - Get the current status of a stock
- GET /data/all-data/<ticker> - Get latest raw stock data JSON for a ticker
"""

import json
import logging
from urllib.parse import urlparse

from django.conf import settings
from django.http import HttpResponse
from minio import Minio
from minio.error import MinioException, S3Error
from rest_framework import status
from rest_framework.decorators import permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from api.models import Stock, StockIngestionRun
from api.serializers import (
    StockSerializer,
    StockStatusResponseSerializer,
)
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    StockNotFoundError,
)


logger = logging.getLogger(__name__)


class TickerDetailView(APIView):
    """
    API endpoint for getting details of a specific stock.
    
    GET /ticker/<ticker>/detail
    
    Returns detailed information about a stock.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, ticker: str) -> Response:
        """
        Get details of a specific stock.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path
            
        Returns:
            Response with stock details or 404 if not found
        """
        try:
            stock = Stock.objects.get(ticker=ticker.upper())
            serializer = StockSerializer(stock)
            logger.info(
                "Stock details retrieved successfully",
                extra={'ticker': ticker.upper(), 'stock_id': str(stock.id)}
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Stock.DoesNotExist:
            logger.warning(
                "Stock not found",
                extra={'ticker': ticker.upper()}
            )
            return Response(
                {
                    'error': {
                        'message': f"Stock with ticker '{ticker.upper()}' not found",
                        'code': 'STOCK_NOT_FOUND',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )


class StockStatusView(APIView):
    """
    API endpoint for getting the current status of a stock.
    
    GET /ticker/<ticker>/status
    
    Returns the stock's latest ingestion run status or 404 if not found.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, ticker: str) -> Response:
        """
        Get the current status of a stock's latest ingestion run.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path
            
        Returns:
            Response with stock status or 404 if not found
        """
        try:
            result = self.service.get_stock_status(ticker)
            serializer = StockStatusResponseSerializer(result)
            logger.info(
                "Stock status retrieved successfully",
                extra={'ticker': ticker.upper(), 'state': result.state, 'run_id': str(result.run_id) if result.run_id else None}
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except StockNotFoundError as e:
            logger.warning(
                "Stock not found",
                extra={'ticker': ticker.upper(), 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'STOCK_NOT_FOUND',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )


class StockDataView(APIView):
    """
    API endpoint for getting the latest raw stock data JSON for a specific ticker.
    
    GET /data/all-data/<ticker>
    
    Returns the latest DONE state ingestion run's raw data JSON exactly as stored in S3/MinIO.
    The response is the raw JSON bytes with no transformation or parsing.
    """
    
    @permission_classes([AllowAny])
    def get(self, request: Request, ticker: str) -> HttpResponse:
        """
        Get the latest raw stock data JSON for a specific ticker.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path (case-insensitive)
            
        Returns:
            HttpResponse with raw JSON bytes (content_type='application/json') or error response
            
        Error Codes:
            - STOCK_NOT_FOUND: Stock with ticker does not exist
            - NO_DONE_RUN_FOUND: No DONE state run exists for the stock
            - NO_RAW_DATA_URI: DONE run exists but raw_data_uri is None or empty
            - INVALID_DATA_URI: S3 URI format is invalid (not s3:// format)
            - DATA_FILE_NOT_FOUND: JSON file not found in S3 (NoSuchKey)
            - STORAGE_AUTHENTICATION_ERROR: S3/MinIO authentication failed
            - STORAGE_BUCKET_NOT_FOUND: S3 bucket not found
            - STORAGE_ERROR: Other S3/MinIO errors
            - STORAGE_CONNECTION_ERROR: MinIO connection error
            - INVALID_JSON_DATA: File contains invalid JSON
            - INTERNAL_SERVER_ERROR: Unexpected errors
        """
        normalized_ticker = ticker.upper()
        minio_response = None
        
        try:
            # Look up stock (case-insensitive)
            try:
                stock = Stock.objects.get(ticker=normalized_ticker)
            except Stock.DoesNotExist:
                logger.warning(
                    "Stock not found for data request",
                    extra={'ticker': normalized_ticker}
                )
                return Response(
                    {
                        'error': {
                            'code': 'STOCK_NOT_FOUND',
                            'message': f"Stock with ticker '{normalized_ticker}' not found",
                            'details': {'ticker': normalized_ticker}
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Query for latest DONE run
            done_run = StockIngestionRun.objects.get_latest_done_run(stock)
            
            if not done_run:
                logger.warning(
                    "No DONE run found for stock",
                    extra={'ticker': normalized_ticker, 'stock_id': str(stock.id)}
                )
                return Response(
                    {
                        'error': {
                            'code': 'NO_DONE_RUN_FOUND',
                            'message': f"No DONE state ingestion run found for ticker '{normalized_ticker}'",
                            'details': {'ticker': normalized_ticker}
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Check raw_data_uri exists
            if not done_run.raw_data_uri:
                logger.warning(
                    "DONE run exists but raw_data_uri is missing",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'stock_id': str(stock.id)
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'NO_RAW_DATA_URI',
                            'message': f"DONE run exists but raw_data_uri is not set for ticker '{normalized_ticker}'",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            raw_data_uri = done_run.raw_data_uri
            
            # Parse S3 URI
            if not raw_data_uri.startswith('s3://'):
                logger.error(
                    "Invalid S3 URI format",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'raw_data_uri': raw_data_uri
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'INVALID_DATA_URI',
                            'message': f"Invalid S3 URI format: {raw_data_uri}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id),
                                'raw_data_uri': raw_data_uri
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Parse S3 URI using urlparse
            parsed_s3_uri = urlparse(raw_data_uri)
            bucket_name = parsed_s3_uri.netloc
            object_key = parsed_s3_uri.path.lstrip('/')
            
            if not bucket_name or not object_key:
                logger.error(
                    "Invalid S3 URI format - cannot parse bucket/key",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'raw_data_uri': raw_data_uri
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'INVALID_DATA_URI',
                            'message': f"Invalid S3 URI format: {raw_data_uri}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id),
                                'raw_data_uri': raw_data_uri
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Initialize MinIO client using base S3 credentials
            parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
            endpoint = parsed.netloc or parsed.path
            secure = parsed.scheme == 'https'
            
            client = Minio(
                endpoint=endpoint,
                access_key=settings.AWS_ACCESS_KEY_ID,
                secret_key=settings.AWS_SECRET_ACCESS_KEY,
                secure=secure
            )

            try:
                minio_response = client.get_object(bucket_name, object_key)
                
                # Read raw JSON bytes
                json_bytes = minio_response.read()
                
                # Validate it's valid JSON by attempting to parse
                try:
                    json.loads(json_bytes)
                except json.JSONDecodeError as e:
                    logger.exception(
                        "Invalid JSON data in file",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            'key': object_key
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'INVALID_JSON_DATA',
                                'message': f"File contains invalid JSON data: {str(e)}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    'key': object_key
                                }
                            }
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
                # Return raw JSON bytes
                logger.info(
                    "Stock data retrieved successfully",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'bucket': bucket_name,
                        'key': object_key
                    }
                )
                return HttpResponse(
                    content=json_bytes,
                    content_type='application/json',
                    status=200
                )
            
            except S3Error as e:
                error_code = e.code
                
                if error_code == 'NoSuchKey':
                    logger.warning(
                        "Data file not found in S3",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            'key': object_key,
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'DATA_FILE_NOT_FOUND',
                                'message': f"Data file not found in storage: {object_key}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    'key': object_key
                                }
                            }
                        },
                        status=status.HTTP_404_NOT_FOUND
                    )
                elif error_code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch', 'AccessDenied']:
                    logger.error(
                        "S3/MinIO authentication error",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_AUTHENTICATION_ERROR',
                                'message': f"S3/MinIO authentication failed: {error_code}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_401_UNAUTHORIZED
                    )
                elif error_code == 'NoSuchBucket':
                    logger.error(
                        "S3 bucket not found",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_BUCKET_NOT_FOUND',
                                'message': f"S3 bucket not found: {bucket_name}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_404_NOT_FOUND
                    )
                else:
                    logger.exception(
                        "S3/MinIO error",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_ERROR',
                                'message': f"S3/MinIO error: {error_code}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
            
            except MinioException as e:
                logger.exception(
                    "MinIO connection error",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id)
                    },
                )
                return Response(
                    {
                        'error': {
                            'code': 'STORAGE_CONNECTION_ERROR',
                            'message': f"MinIO connection error: {str(e)}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            finally:
                # Always close the MinIO response if it was opened
                if minio_response:
                    try:
                        minio_response.close()
                        minio_response.release_conn()
                    except Exception as cleanup_error:
                        logger.debug(
                            "Error during MinIO response cleanup",
                            extra={
                                'error': str(cleanup_error),
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        )
        
        except Exception as e:
            logger.exception(
                "Unexpected error retrieving stock data",
                extra={
                    'ticker': normalized_ticker,
                },
            )
            return Response(
                {
                    'error': {
                        'code': 'INTERNAL_SERVER_ERROR',
                        'message': f"An unexpected error occurred: {str(e)}",
                        'details': {
                            'ticker': normalized_ticker
                        }
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
