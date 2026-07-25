import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

import django

django.setup()

from myapp.types import Payload  # noqa: E402

from djoutbox import publish  # noqa: E402


def callback(payload: Payload, routing_key: str, attempt_count: int) -> None:
    if attempt_count <= payload.exception_count:
        raise Exception(
            f"Attempt {attempt_count}/{payload.exception_count + 1} "
            f"failed for '{payload.message}' on '{routing_key}'"
        )

    print(f"[{routing_key}] {payload.message} (attempt {attempt_count})")

    if payload.second_message:
        print(f"  → publishing second message to '{payload.second_message_routing_key}'")
        publish(
            payload.second_message_routing_key,
            Payload(message=payload.second_message),
        )
