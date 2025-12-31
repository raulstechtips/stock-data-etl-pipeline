from django.views.generic import TemplateView


class LoginView(TemplateView):
    """
    Login view - displays authentication page with Keycloak SSO
    """
    template_name = 'frontend/auth/login.html'
