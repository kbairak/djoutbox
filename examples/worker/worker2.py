import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

import django  # noqa: E402

django.setup()

import asyncio  # noqa: E402

from django.conf import settings  # noqa: E402

from djoutbox import Consumer, Worker  # noqa: E402
from worker.callback import callback  # noqa: E402

if __name__ == "__main__":
    worker = Worker(
        consumers=[
            Consumer("two", callback, "worker2_two", ("1s", "2s", "3s")),
            Consumer("three", callback, "worker2_three", ("3s", "6s")),
        ],
        **settings.DJOUTBOX,
    )
    asyncio.run(worker.run())
