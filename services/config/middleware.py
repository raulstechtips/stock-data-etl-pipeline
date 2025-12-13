"""
Custom middleware for the Django application.
"""

from django.http import HttpResponse


def health_check_middleware(get_response):
    """
    Middleware to handle health check requests at /health/.
    
    Returns a simple 200 OK response for health check requests
    without hitting the database or other services.
    """
    def middleware(request):
        if request.path == '/health/':
            return HttpResponse('Healthy!', content_type='text/plain', status=200)
        return get_response(request)
    
    return middleware

