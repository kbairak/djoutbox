import asyncio

from django.core.management.base import BaseCommand

from djoutbox import Relay
from djoutbox.conf import build_dsn


class Command(BaseCommand):
    help = "Run the djoutbox relay process"

    def handle(self, *args, **options):
        from django.conf import settings
        asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())
