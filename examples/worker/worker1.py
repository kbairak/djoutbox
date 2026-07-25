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
            Consumer("one", callback, "worker1_one", ("1s", "3s", "5s")),
            Consumer("two", callback, "worker1_two", ("2s", "4s")),
        ],
        **settings.DJOUTBOX,
    )
    asyncio.run(worker.run())
