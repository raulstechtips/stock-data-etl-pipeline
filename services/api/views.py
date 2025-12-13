"""
API Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- GET /ticker/<ticker>/status - Get the current status of a stock
- POST /ticker/queue - Queue a stock for ingestion
- PATCH /runs/<run_id>/state - Update a run's state (for internal services)
"""

import logging
from uuid import UUID

from django.db import IntegrityError
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from api.serializers import (
    QueueForFetchRequestSerializer,
    StockIngestionRunSerializer,
    StockStatusResponseSerializer,
    UpdateRunStateRequestSerializer,
)
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    InvalidStateTransitionError,
    StockNotFoundError,
)


logger = logging.getLogger(__name__)


class StockStatusView(APIView):
    """
    API endpoint for getting the current status of a stock.
    
    GET /ticker/<ticker>/status
    
    Returns the stock's latest ingestion run status or 404 if not found.
    """
    permission_classes = [AllowAny]
    
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
                extra={'ticker': ticker.upper(), 'status': result.get('status')}
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


class QueueForFetchView(APIView):
    """
    API endpoint for queuing a stock for ingestion.
    
    POST /ticker/queue
    
    Creates the stock if it doesn't exist, then either:
    - Returns the existing active run if one exists
    - Creates a new run in QUEUED_FOR_FETCH state
    """
    permission_classes = [AllowAny]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def post(self, request: Request) -> Response:
        """
        Queue a stock for fetching.
        
        Request body:
            {
                "ticker": "AAPL",
                "requested_by": "user@example.com",  // optional
                "request_id": "unique-request-123"   // optional
            }
        
        Returns:
            - 200: If an active run already exists (returns existing run)
            - 201: If a new run was created
            - 400: If the request is invalid
        """
        serializer = QueueForFetchRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(
                "Queue for fetch validation failed",
                extra={'errors': serializer.errors, 'data': request.data}
            )
            return Response(
                {
                    'error': {
                        'message': 'Validation failed',
                        'code': 'VALIDATION_ERROR',
                        'details': serializer.errors
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        validated_data = serializer.validated_data
        ticker = validated_data['ticker']
        requested_by = validated_data.get('requested_by')
        request_id = validated_data.get('request_id')
        
        try:
            run, created = self.service.queue_for_fetch(
                ticker=ticker,
                requested_by=requested_by,
                request_id=request_id,
            )
        except IntegrityError:
            # Race condition: Another request created a run between our check and create
            logger.exception(
                "Race condition detected while queuing stock for fetch",
                extra={
                    'ticker': ticker.upper(),
                    'requested_by': requested_by,
                    'request_id': request_id,
                }
            )
            return Response(
                {
                    'error': {
                        'message': 'An ingestion run for this stock was created by another request. Please try again.',
                        'code': 'RACE_CONDITION',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_409_CONFLICT
            )
        
        response_serializer = StockIngestionRunSerializer(run)
        
        if created:
            logger.info(
                "Stock queued for fetch successfully - new run created",
                extra={
                    'ticker': ticker.upper(),
                    'run_id': str(run.id),
                    'requested_by': requested_by,
                    'request_id': request_id
                }
            )
            return Response(
                response_serializer.data,
                status=status.HTTP_201_CREATED
            )
        else:
            # Active run already exists
            logger.info(
                "Stock already queued - returning existing run",
                extra={
                    'ticker': ticker.upper(),
                    'run_id': str(run.id),
                    'run_state': run.state,
                    'requested_by': requested_by
                }
            )
            return Response(
                response_serializer.data,
                status=status.HTTP_200_OK
            )


class UpdateRunStateView(APIView):
    """
    API endpoint for updating the state of an ingestion run.
    
    PATCH /runs/<run_id>/state
    
    Used by internal services to update the state of a run
    as it progresses through the ETL pipeline.
    """
    permission_classes = [AllowAny]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def patch(self, request: Request, run_id: str) -> Response:
        """
        Update the state of an ingestion run.
        
        Request body:
            {
                "state": "FETCHING",
                "error_code": "FETCH_TIMEOUT",     // required if FAILED
                "error_message": "Connection timed out",  // required if FAILED
                "raw_data_uri": "s3://bucket/...", // optional
                "processed_data_uri": "..."        // optional
            }
        
        Returns:
            - 200: State updated successfully
            - 400: Invalid request or state transition
            - 404: Run not found
        """
        # Validate UUID format
        try:
            run_uuid = UUID(run_id)
        except ValueError as e:
            logger.warning(
                "Invalid UUID format in update run state request",
                extra={'run_id': run_id, 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': f"Invalid run ID format: '{run_id}'",
                        'code': 'INVALID_UUID',
                        'details': {'run_id': run_id}
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        serializer = UpdateRunStateRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(
                "Update run state validation failed",
                extra={'run_id': run_id, 'errors': serializer.errors, 'data': request.data}
            )
            return Response(
                {
                    'error': {
                        'message': 'Validation failed',
                        'code': 'VALIDATION_ERROR',
                        'details': serializer.errors
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        validated_data = serializer.validated_data
        
        try:
            run = self.service.update_run_state(
                run_id=run_uuid,
                new_state=validated_data['state'],
                error_code=validated_data.get('error_code'),
                error_message=validated_data.get('error_message'),
                raw_data_uri=validated_data.get('raw_data_uri'),
                processed_data_uri=validated_data.get('processed_data_uri'),
            )
            
            logger.info(
                "Run state updated successfully",
                extra={
                    'run_id': str(run_uuid),
                    'new_state': validated_data['state'],
                    'ticker': run.stock.ticker,
                    'error_code': validated_data.get('error_code'),
                }
            )
            
            response_serializer = StockIngestionRunSerializer(run)
            return Response(
                response_serializer.data,
                status=status.HTTP_200_OK
            )
            
        except IngestionRunNotFoundError as e:
            logger.warning(
                "Ingestion run not found for state update",
                extra={'run_id': str(run_uuid), 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'RUN_NOT_FOUND',
                        'details': {'run_id': str(run_uuid)}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )
            
        except InvalidStateTransitionError as e:
            logger.warning(
                "Invalid state transition attempted",
                extra={
                    'run_id': str(run_uuid),
                    'requested_state': validated_data['state'],
                    'error': str(e)
                }
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'INVALID_STATE_TRANSITION',
                        'details': {'run_id': str(run_uuid)}
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )


class RunDetailView(APIView):
    """
    API endpoint for getting details of a specific ingestion run.
    
    GET /runs/<run_id>
    
    Returns detailed information about an ingestion run.
    """
    permission_classes = [AllowAny]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, run_id: str) -> Response:
        """
        Get details of a specific ingestion run.
        
        Args:
            request: DRF Request object
            run_id: UUID of the ingestion run
            
        Returns:
            Response with run details or 404 if not found
        """
        # Validate UUID format
        try:
            run_uuid = UUID(run_id)
        except ValueError as e:
            logger.warning(
                "Invalid UUID format in run detail request",
                extra={'run_id': run_id, 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': f"Invalid run ID format: '{run_id}'",
                        'code': 'INVALID_UUID',
                        'details': {'run_id': run_id}
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            run = self.service.get_run_by_id(run_uuid)
            serializer = StockIngestionRunSerializer(run)
            logger.info(
                "Run details retrieved successfully",
                extra={
                    'run_id': str(run_uuid),
                    'ticker': run.stock.ticker,
                    'state': run.state
                }
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except IngestionRunNotFoundError as e:
            logger.warning(
                "Ingestion run not found",
                extra={'run_id': str(run_uuid), 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'RUN_NOT_FOUND',
                        'details': {'run_id': str(run_uuid)}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )
