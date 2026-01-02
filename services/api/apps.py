from django.apps import AppConfig


class ApiConfig(AppConfig):
    """
    Configuration for the API app.
    
    This app configuration ensures that Django signals are registered
    when the app is ready. Signals are imported in the ready() method
    to ensure they are connected to their respective models.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'
    
    def ready(self):
        """
        Register Django signals when the app is ready.
        
        This method is called when Django starts up and the app is ready.
        Importing the signals module ensures that signal handlers are
        registered and connected to their respective models.
        """
        # Import signals to ensure signal handlers are registered
        from api import signals  # noqa: F401
