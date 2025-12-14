"""
Custom middleware for the Django application.
"""
from typing import Callable
from django.http import HttpResponse
from django.http import HttpRequest

def health_check_middleware(get_response: Callable) -> Callable:
    """
    Middleware to handle health check requests at /health/.
    
    Returns a simple 200 OK response for health check requests
    without hitting the database or other services.
    """
    def middleware(request: HttpRequest) -> HttpResponse:
        if request.path == '/health/':
            return HttpResponse('Healthy!', content_type='text/plain', status=200)
        return get_response(request)
    
    return middleware

