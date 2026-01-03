import json
import io
from urllib.parse import urlparse

from django.conf import settings
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
from minio import Minio
from minio.error import MinioException, S3Error


@require_http_methods(["GET"])
def mock_stock_data_api(request, ticker):
    """
    Mock API endpoint that serves stock data from MinIO/S3 storage.
    
    This view is used for testing the queue_for_fetch worker without hitting
    the actual external API. It fetches stock data from MinIO at the path
    bucket/{TICKER}.JSON and returns it as a JSON response.
    
    Query Parameters:
        ticker: Stock ticker symbol (required)
    
    Returns:
        HttpResponse: The JSON data from the S3/MinIO bucket
    
    Status Codes:
        200: Success
        401: Authentication failed
        404: File not found in bucket
        500: Connection error or other server error
    """
    # Uppercase ticker for the filename
    ticker_upper = ticker.upper()
    
    try:
        # Parse endpoint URL to extract hostname and determine if secure
        parsed = urlparse(settings.MOCK_STOCK_API_AWS_S3_ENDPOINT_URL)
        endpoint = parsed.netloc or parsed.path
        secure = parsed.scheme == 'https'
        
        # Initialize MinIO client
        client = Minio(
            endpoint=endpoint,
            access_key=settings.MOCK_STOCK_API_AWS_ACCESS_KEY_ID,
            secret_key=settings.MOCK_STOCK_API_AWS_SECRET_ACCESS_KEY,
            secure=secure
        )
        
        # Get bucket name (using STOCK_RAW_DATA_BUCKET as it's the stock data bucket)
        bucket_name = settings.MOCK_STOCK_DATA_BUCKET
        
        # Construct object key: bucket/{TICKER}.JSON
        object_key = f"{ticker_upper}.json"
        
        # Fetch object from MinIO
        response = client.get_object(bucket_name, object_key)
        
        try:
            # Read the raw JSON data from MinIO
            json_data = response.read()
            
            # Validate it's valid JSON by attempting to parse
            json.loads(json_data.decode('utf-8'))
            
            # Return raw JSON bytes directly (same as what MinIO stored)
            # This avoids unnecessary parse/serialize cycle
            return HttpResponse(
                json_data,
                content_type='application/json'
            )
        
        finally:
            # Always close the response to release resources
            response.close()
            response.release_conn()
    
    except S3Error as e:
        # Handle S3-specific errors
        if e.code == 'NoSuchKey':
            error_response = json.dumps({'error': f'Stock data not found for ticker: {ticker_upper}'})
            return HttpResponse(error_response, content_type='application/json', status=404)
        elif e.code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch', 'AccessDenied']:
            error_response = json.dumps({'error': f'S3/MinIO authentication failed: {e.code}'})
            return HttpResponse(error_response, content_type='application/json', status=401)
        elif e.code == 'NoSuchBucket':
            error_response = json.dumps({'error': f'S3/MinIO bucket not found: {bucket_name}'})
            return HttpResponse(error_response, content_type='application/json', status=404)
        else:
            error_response = json.dumps({'error': f'S3/MinIO error: {e.code}'})
            return HttpResponse(error_response, content_type='application/json', status=500)
    
    except MinioException as e:
        # Handle MinIO connection errors
        error_response = json.dumps({'error': f'MinIO connection error: {str(e)}'})
        return HttpResponse(error_response, content_type='application/json', status=500)
    
    except (json.JSONDecodeError, ValueError) as e:
        # Handle invalid JSON in the file
        error_response = json.dumps({'error': f'Invalid JSON in stock data file: {str(e)}'})
        return HttpResponse(error_response, content_type='application/json', status=500)
    
    except Exception as e:
        # Handle any other unexpected errors
        error_response = json.dumps({'error': f'Error fetching stock data: {str(e)}'})
        return HttpResponse(error_response, content_type='application/json', status=500)
