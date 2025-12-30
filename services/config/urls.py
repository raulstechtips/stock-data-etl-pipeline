"""
URL configuration for services project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
# from django.contrib import admin
from django.urls import include, path
from django.shortcuts import redirect

def root_redirect(request):
    """Smart root redirect that checks authentication status and redirects to the appropriate page"""
    if not request.user.is_authenticated:
        return redirect('frontend:login')
    return redirect('frontend:stocks-list')

def blocked_login_redirect(request):
    """Redirect blocked allauth login pages to custom login"""
    return redirect('frontend:login')

urlpatterns = [
    # path("admin/", admin.site.urls),

    # Root redirect
    path('', root_redirect, name='root_redirect'),

    # Allauth blocked login redirects for non keycloak users
    path('accounts/login/', blocked_login_redirect, name='account_login_blocked'),
    path('accounts/signup/', blocked_login_redirect, name='account_signup_blocked'),
    path('accounts/3rdparty/', blocked_login_redirect, name='account_3rdparty_blocked'),
    path('accounts/password/reset/', blocked_login_redirect, name='account_password_reset_blocked'),
    path('accounts/password/change/', blocked_login_redirect, name='account_password_change_blocked'),
    path('accounts/email/', blocked_login_redirect, name='account_email_blocked'),

    # Allauth URLs at root level for provider_login_url to work for keycloak users
    path('accounts/', include('allauth.urls')),  
    
    # API
    path('api/', include('api.urls')),
    
    # Frontend
    path('dashboard/', include('frontend.urls')),
]
