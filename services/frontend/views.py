from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin


class LoginView(TemplateView):
    """
    Login view - displays authentication page with Keycloak SSO
    """
    template_name = 'frontend/auth/login.html'


class StocksListView(LoginRequiredMixin, TemplateView):
    """
    Stocks list view - displays all stocks with filtering and pagination
    """
    template_name = 'frontend/stocks/list.html'

class IngestionRunsListView(LoginRequiredMixin, TemplateView):
    """
    Ingestion runs list view - displays all ingestion runs with filtering and pagination
    """
    template_name = 'frontend/ingestion_runs/list.html'

class BulkQueueRunsListView(LoginRequiredMixin, TemplateView):
    """
    Bulk queue runs list view - displays all bulk queue runs with filtering and pagination
    """
    template_name = 'frontend/bulk_queue_runs/list.html'
