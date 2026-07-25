import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

import asyncio  # noqa: E402

from djoutbox import Relay  # noqa: E402
from djoutbox.conf import build_dsn  # noqa: E402

if __name__ == "__main__":
    from django.conf import settings

    asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())
