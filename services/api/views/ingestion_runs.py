"""
API Ingestion Run Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- POST /ticker/queue - Queue a stock for ingestion
- GET /run/<run_id>/detail - Get details of a specific run
"""

import logging
from uuid import UUID

from django.db import IntegrityError
from celery.exceptions import CeleryError, OperationalError as CeleryOperationalError
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from api.models import IngestionState
from api.serializers import (
    QueueForFetchRequestSerializer,
    StockIngestionRunSerializer,
)
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
)
from workers.tasks.queue_for_fetch import fetch_stock_data


logger = logging.getLogger(__name__)


class QueueForFetchView(APIView):
    """
    API endpoint for queuing a stock for ingestion.
    
    POST /ticker/queue
    
    Creates the stock if it doesn't exist, then either:
    - Returns the existing active run if one exists
    - Creates a new run in QUEUED_FOR_FETCH state
    """
    
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
            - 201: If a new run was created and queued successfully
            - 400: If the request is invalid
            - 500: If failed to queue the task to message broker
        """
        serializer = QueueForFetchRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(
                "Queue for fetch validation failed",
                extra={'errors': serializer.errors}
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
            logger.warning(
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
        
        # If a new run was created, trigger the Celery task
        if created:
            try:
                # Send task to message broker
                # Note: We call this AFTER the transaction commits to ensure
                # the run exists in the database before the worker processes it
                task_result = fetch_stock_data.delay(
                    run_id=str(run.id),
                    ticker=ticker
                )

                logger.info(
                    "Stock queued for fetch successfully - new run created",
                    extra={
                        'ticker': ticker.upper(),
                        'run_id': str(run.id),
                        'task_id': task_result.id,
                        'requested_by': requested_by,
                        'request_id': request_id
                    }
                )
                
                response_serializer = StockIngestionRunSerializer(run)
                return Response(
                    response_serializer.data,
                    status=status.HTTP_201_CREATED
                )
                
            except (CeleryError, CeleryOperationalError) as e:
                # Failed to send task to message broker
                logger.exception(
                    "Failed to queue fetch task for run",
                    extra={
                        'run_id': str(run.id),
                        'ticker': ticker.upper(),
                        'requested_by': requested_by,
                        'request_id': request_id
                    }
                )
                
                # Transition the run to FAILED state since we can't process it
                try:
                    self.service.update_run_state(
                        run_id=run.id,
                        new_state=IngestionState.FAILED,
                        error_code='BROKER_ERROR',
                        error_message=f'Failed to queue task to message broker: {str(e)}'
                    )
                except Exception as state_error:
                    logger.exception(
                        "Failed to transition run to FAILED after broker error",
                        extra={
                            'run_id': str(run.id),
                            'ticker': ticker.upper(),
                            'requested_by': requested_by,
                            'request_id': request_id,
                        }
                    )
                
                return Response(
                    {
                        'message': 'Failed to queue task to message broker',
                        'code': 'BROKER_ERROR',
                        'details': {'run_id': str(run.id)}
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        else:
            # Active run already exists, don't trigger a new task
            logger.debug(
                "Stock already queued - returning existing run",
                extra={
                    'ticker': ticker.upper(),
                    'run_id': str(run.id),
                    'run_state': run.state,
                    'requested_by': requested_by
                }
            )
            
            response_serializer = StockIngestionRunSerializer(run)

            return Response(
                response_serializer.data,
                status=status.HTTP_200_OK
            )



class RunDetailView(APIView):
    """
    API endpoint for getting details of a specific ingestion run.
    
    GET /run/<run_id>/detail
    
    Returns detailed information about an ingestion run.
    """
    
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
            logger.debug(
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
