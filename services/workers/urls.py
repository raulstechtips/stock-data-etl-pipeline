"""
URL configuration for workers app.
"""
from django.urls import path
from workers import views

urlpatterns = [
    path('stock-data/<str:ticker>', views.mock_stock_data_api, name='mock_stock_data_api'),
]
