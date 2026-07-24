from django.apps import AppConfig


class DjoutboxConfig(AppConfig):
    name = "djoutbox"
    verbose_name = "Djoutbox"

    def ready(self):
        from djoutbox.conf import validate_settings

        validate_settings()
