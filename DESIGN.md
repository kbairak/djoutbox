# Custom serializers/deserializers

Replace hardcoded pydantic `BaseModel` special-case in publisher + worker with a pluggable type registry.

## Registry

No module-level list. Publisher and worker read serializers from their own source at point of use.

## Publisher

`_serialize_message` looks up serializers from Django settings each call:

```python
def _serialize_message(msg, now, tracking_ids):
    from django.conf import settings

    serializers = settings.DJOUTBOX.get("serializers", [])
    for type_, serializer, _ in serializers:
        if isinstance(msg.body, type_):
            body_bytes = serializer(msg.body)
            break
    else:
        if isinstance(msg.body, bytes):
            body_bytes = msg.body
        else:
            body_bytes = json.dumps(msg.body).encode()
    ...
```

## Worker

Worker init accepts `serializers` kwarg. `Consumer._handle` uses it for deserialization:

```python
@dataclass
class Worker:
    ...
    serializers: Sequence | None = None
```

`Consumer._handle(message)` checks worker's serializers:

```python
for type_, _, deserializer in (worker.serializers or []):
    if inspect.isclass(body_type) and issubclass(body_type, type_):
        body = deserializer(body_type, message.body)
        break
else:
    if inspect.isclass(body_type) and issubclass(body_type, bytes):
        body = message.body
    else:
        body = json.loads(message.body)
```

The management command passes `settings.DJOUTBOX["serializers"]` to Worker:

```python
class Command(BaseCommand):
    def handle(self, *args, **options):
        worker = Worker(
            consumers=[...],
            serializers=settings.DJOUTBOX.get("serializers", []),
            **settings.DJOUTBOX,
        )
        asyncio.run(worker.run())
```

## Example: pydantic

```python
DJOUTBOX = {
    "serializers": [
        (BaseModel, lambda m: m.model_dump_json().encode(), lambda cls, d: cls.model_validate_json(d)),
    ]
}
```

## Removing pydantic from optional deps

`pyproject.toml`: drop `[project.optional-dependencies] pydantic`. Move to dev deps only.

## What does NOT change

- Plain dict/list/str/... still serialize via `json.dumps`/`json.loads`
- No schema migration — `PendingMessage`/`SentMessage` unchanged
- No `content_type` field — deserialization chosen by consumer's type annotation, not wire marker