from django.urls import path
from .views import LoginView, StocksListView, IngestionRunsListView, BulkQueueRunsListView

app_name = 'frontend'

urlpatterns = [
    # Authentication views
    path('login/', LoginView.as_view(), name='login'),
    
    # Stocks views
    path('stocks/', StocksListView.as_view(), name='stocks-list'),

    # Ingestion runs views
    path('runs/', IngestionRunsListView.as_view(), name='ingestion-runs-list'),
    
    # Bulk queue runs views
    path('bulk/runs/', BulkQueueRunsListView.as_view(), name='bulk-queue-runs-list'),
]