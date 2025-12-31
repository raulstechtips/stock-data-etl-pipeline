from django.urls import path
from .views import LoginView

app_name = 'frontend'

urlpatterns = [
    # Authentication views
    path('login/', LoginView.as_view(), name='login'),
]