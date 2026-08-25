# PLAN: Pluggable serializer/deserializer registry

## Goal

Replace hardcoded pydantic special-case in `djoutbox/publisher.py` and `djoutbox/worker.py` with a pluggable type registry configured via Django settings. Drop pydantic as an optional runtime dependency — it becomes a dev/test-only dep and a documented example of the new registry mechanism.

## Implementer context

You are executing this plan in a fresh session. All decisions are pre-made below —
do not redesign, rename, or "improve" anything. When you need knowledge you don't
have (API signatures, existing helpers, library docs), look it up rather than assume.
If the plan conflicts with reality (file missing, signature differs), STOP and report
the conflict instead of improvising.

## Assumptions

- `djoutbox/publisher.py` exists; `_serialize_message` at line 32 contains the hardcoded `isinstance(msg.body, BaseModel)` branch (line 36).
- `djoutbox/worker.py` exists; `Worker` is a `@dataclass` at line 282; `Consumer._handle` at line 76 contains the hardcoded `issubclass(body_type, BaseModel)` branch (line 122).
- `djoutbox/conf.py` holds `DEFAULTS` dict (line 11) and `get_setting(key)` helper (line 80); `validate_settings` at line 85.
- Repo has NO built-in management command. Docs (docs/worker.md line 92-114) show user-written commands using `Worker(consumers=[...], **settings.DJOUTBOX)` pattern.
- `pyproject.toml` has `[project.optional-dependencies] pydantic = ["pydantic>=2,<3"]` at line 20. Pydantic is already in `[dependency-groups] dev` at line 57.
- Tests: pytest with `pytest-asyncio`, `pytest-django`, testcontainers. Run via `pytest` from repo root (Makefile: `pytest --cov --cov-report=term-missing -v`). Test paths: `src/tests/`.
- `src/tests/utils.py` defines `Person(BaseModel)` used by `test_publisher.py:59` (`test_serialize_pydantic`).
- `src/tests/conftest.py` calls `settings.configure(DJOUTBOX={}, ...)` at line 28.
- Examples: `examples/worker/worker1.py` and `examples/worker/worker2.py` splat `**settings.DJOUTBOX` into `Worker(...)`. `examples/myproject/settings.py` defines `DJOUTBOX = {"rmq_url": ...}`. `examples/myapp/types.py` defines `Payload(BaseModel)`.
- Python 3.10+; `from __future__ import annotations` already in all source files.

## Out of scope

- No new management command. Docs pattern already supports passing `serializers` via `**settings.DJOUTBOX` once `Worker` accepts the kwarg.
- No `content_type` wire marker. Deserialization chosen by consumer's type annotation only.
- No changes to `PendingMessage`/`SentMessage` models or migrations.
- No changes to `Relay`.
- No conversion of `Worker` from dataclass to custom `__init__` with `**_ignored`. Users with extra keys in `DJOUTBOX` dict still get TypeError — same as today.
- No mkdocs site rebuild (leave to CI/user).

## Steps

### Track A: Core registry (independent of docs/examples)

1. **Add `serializers` setting to defaults and validation**
   - Files: `src/djoutbox/conf.py` (edit)
   - Change: Add `"serializers": ()` entry to `DEFAULTS` dict (line 11). Do NOT add validation of its shape — callables are opaque to validate_settings.
   - Verify: `python -c "from djoutbox.conf import DEFAULTS; assert DEFAULTS['serializers'] == ()"` (run with `uv run python`)

2. **Publisher: look up serializers from settings**
   - Files: `src/djoutbox/publisher.py` (edit)
   - Change:
     - Delete `try: from pydantic import BaseModel ...` block (lines 17-21).
     - Rewrite `_serialize_message` (line 32) body branch:
       ```python
       from django.conf import settings
       serializers = getattr(settings, "DJOUTBOX", {}).get("serializers", ())
       for type_, serializer, _ in serializers:
           if isinstance(msg.body, type_):
               body_bytes = serializer(msg.body)
               break
       else:
           if isinstance(msg.body, bytes):
               body_bytes = msg.body
           else:
               body_bytes = json.dumps(msg.body).encode()
       ```
     - Keep surrounding `try/except (TypeError, ValueError)` wrapper (lines 42-46) intact — applies to serializer callables too.
     - Do not import `get_setting` from conf — inline `getattr(settings, "DJOUTBOX", {}).get(...)` keeps it lazy per DESIGN.md ("looks up serializers from Django settings each call").
   - Verify: `uv run python -c "from djoutbox.publisher import _serialize_message; print('ok')"`

3. **Worker: add `serializers` field + use in `_handle`**
   - Files: `src/djoutbox/worker.py` (edit)
   - Change:
     - Delete `try: from pydantic import BaseModel ...` block (lines 32-36).
     - Add field to `Worker` dataclass (around line 289, after `prefetch_count`):
       ```python
       serializers: Sequence | None = None
       ```
       `Sequence` is already imported (line 9).
     - In `Consumer._handle`, replace `if inspect.isclass(body_type) and issubclass(body_type, BaseModel): body = body_type.model_validate_json(message.body)` branch (lines 122-127) with:
       ```python
       body = message.body  # default fallback
       for type_, _, deserializer in (worker_serializers or ()):
           if inspect.isclass(body_type) and issubclass(body_type, type_):
               body = deserializer(body_type, message.body)
               break
       else:
           if inspect.isclass(body_type) and issubclass(body_type, bytes):
               body = message.body
           else:
               body = json.loads(message.body)
       ```
     - `_handle` is a method on `Consumer`, not `Worker`. To reach worker's serializers, add a `_worker_serializers: Sequence | None = None` private field on `Consumer` (next to `_exchange_name` line 48), and populate it in `Worker._set_up_queues` next to `consumer._exchange_name = self.exchange_name` (line 429):
       ```python
       consumer._worker_serializers = self.serializers
       ```
       Then in `_handle`, reference `self._worker_serializers`.
     - Keep the existing `try/except Exception` wrapping the deserialization block intact (lines 121-141) — serializer exceptions must still nack + metrics.
   - Verify: `uv run python -c "from djoutbox.worker import Worker, Consumer; w = Worker(serializers=[(dict, lambda d: b'', lambda c, b: c())]); print('ok')"`

4. **Drop pydantic optional-dependency from pyproject**
   - Files: `pyproject.toml` (edit)
   - Change: Delete line 20 `pydantic = ["pydantic>=2,<3"]` from `[project.optional-dependencies]`. Keep the section itself with `psycopg` entry. Do NOT remove pydantic from `[dependency-groups] dev` (line 57).
   - Verify: `grep -n "pydantic" pyproject.toml` shows only line in `[dependency-groups] dev`.

### Track B: Tests (depends on Track A)

5. **Update publisher pydantic test to use registry**
   - Files: `src/tests/test_publisher.py` (edit)
   - Change: Rewrite `test_serialize_pydantic` (line 59) to register pydantic via `settings.DJOUTBOX["serializers"]` and restore after:
     ```python
     from django.conf import settings
     from pydantic import BaseModel

     @pytest.mark.django_db
     def test_serialize_pydantic():
         class Person(BaseModel):
             name: str
         serializers = [(
             BaseModel,
             lambda m: m.model_dump_json().encode(),
             lambda cls, d: cls.model_validate_json(d),
         )]
         settings.DJOUTBOX = {**settings.DJOUTBOX, "serializers": serializers}
         try:
             bulk_publish([OutboxMessage("k", Person(name="Alice"))])
         finally:
             settings.DJOUTBOX = {k: v for k, v in settings.DJOUTBOX.items() if k != "serializers"}

         msg = PendingMessage.objects.first()
         assert msg is not None
         assert json.loads(bytes(msg.body)) == {"name": "Alice"}
     ```
     Then delete the `from .utils import Person` import (line 12) if no other test uses it (verify with grep — currently only line 60).
   - Verify: `uv run pytest src/tests/test_publisher.py::test_serialize_pydantic -v` passes.

6. **Update test_imports.py**
   - Files: `src/tests/test_imports.py` (edit)
   - Change: Delete `test_pydantic_import_guard` (lines 4-7) — `djoutbox.publisher.BaseModel` no longer exists. Keep `test_publisher_exports_lazy` (lines 10-13) unchanged.
   - Verify: `uv run pytest src/tests/test_imports.py -v` passes.

7. **Remove `Person` from tests/utils.py if unused**
   - Files: `src/tests/utils.py` (edit)
   - Change: If step 5 removed the only `Person` import, delete the `Person(BaseModel)` class (lines 8-15) and the `from pydantic import BaseModel` import. Keep other helpers (`run_worker`, `get_dlq_message_count`) intact.
   - Verify: `grep -rn "Person" src/tests/` returns zero matches.

8. **Add worker deserialization registry test**
   - Files: `src/tests/test_worker.py` (edit)
   - Change: Add new test at end of file:
     ```python
     @pytest.mark.asyncio
     async def test_consumer_deserializes_via_registry(worker: Worker):
         from pydantic import BaseModel

         class Payload(BaseModel):
             name: str

         received = []

         @consume(binding_key="k", queue_name="test_registry_deser")
         async def handler(payload: Payload) -> None:
             received.append(payload)

         worker.serializers = [(
             BaseModel,
             lambda m: m.model_dump_json().encode(),
             lambda cls, d: cls.model_validate_json(d),
         )]

         # Build a fake incoming message that quacks like AbstractIncomingMessage
         class FakeMessage:
             body = b'{"name": "Bob"}'
             routing_key = "k"
             delivery_tag = 1
             headers = {}
             content_type = "application/json"
             async def ack(self): pass
             async def nack(self, requeue=False): pass

         consumer = handler  # @consume returns Consumer instance
         consumer._exchange_name = "outbox"
         consumer._worker_serializers = worker.serializers
         await consumer._handle(FakeMessage())

         assert len(received) == 1
         assert received[0].name == "Bob"
     ```
     Adjust import of `Worker` at top of file (already there: `from djoutbox import Consumer, Worker, consume`). The `worker` fixture comes from `conftest.py` (line 98).
   - Verify: `uv run pytest src/tests/test_worker.py::test_consumer_deserializes_via_registry -v` passes.

9. **Add worker default-bytes test (regression)**
   - Files: `src/tests/test_worker.py` (edit)
   - Change: Add test verifying `bytes` type hint still short-circuits:
     ```python
     @pytest.mark.asyncio
     async def test_consumer_bytes_passthrough(worker: Worker):
         received = []

         @consume(binding_key="k", queue_name="test_bytes_passthrough")
         async def handler(payload: bytes) -> None:
             received.append(payload)

         class FakeMessage:
             body = b"raw bytes"
             routing_key = "k"
             delivery_tag = 1
             headers = {}
             content_type = "application/octet-stream"
             async def ack(self): pass
             async def nack(self, requeue=False): pass

         consumer = handler
         consumer._exchange_name = "outbox"
         consumer._worker_serializers = None
         await consumer._handle(FakeMessage())

         assert received == [b"raw bytes"]
     ```
   - Verify: `uv run pytest src/tests/test_worker.py::test_consumer_bytes_passthrough -v` passes.

### Track C: Docs + examples (depends on Track A)

10. **Update installation.md — remove pydantic extra, document serializers setting**
    - Files: `docs/installation.md` (edit)
    - Change:
      - Delete the "Optional dependencies → Pydantic" subsection (lines 22-28).
      - In "Configure DJOUTBOX settings" code block (line 44), add commented example:
        ```python
        "serializers": (),  # see "Custom serializers" below
        ```
      - Add new section after "Settings reference" table (after line 92):
        ```markdown
        ### Custom serializers

        `serializers` is a sequence of `(type, serializer, deserializer)` tuples consulted by both publisher and worker:

        ```python
        from pydantic import BaseModel

        DJOUTBOX = {
            ...,
            "serializers": [
                (BaseModel,
                 lambda m: m.model_dump_json().encode(),
                 lambda cls, d: cls.model_validate_json(d)),
            ],
        }
        ```

        - Publisher: first tuple whose `type` matches `isinstance(body, type)` is used; falls back to `bytes` passthrough, then `json.dumps`.
        - Worker: first tuple whose `type` matches `issubclass(body_type_hint, type)` is used; falls back to `bytes` passthrough, then `json.loads`.
        ```
    - Verify: `grep -n "pydantic\[" docs/installation.md` returns no matches; `grep -n "serializers" docs/installation.md` returns at least one match.

11. **Update publishing.md serialization section**
    - Files: `docs/publishing.md` (edit)
    - Change: Replace "Serialization" section (lines 51-59) with:
      ```markdown
      ## Serialization

      The body is serialized as follows, in order:

      1. **Custom serializers** — if `DJOUTBOX["serializers"]` is configured, the first tuple whose `type` matches `isinstance(body, type)` is used.
      2. **`bytes`** — used as-is.
      3. **Anything else** — serialized via `json.dumps().encode()`.

      Unserializable values raise `ValueError` with the routing key in the message.

      Example — register Pydantic:

      ```python
      # settings.py
      from pydantic import BaseModel

      DJOUTBOX = {
          ...,
          "serializers": [(
              BaseModel,
              lambda m: m.model_dump_json().encode(),
              lambda cls, d: cls.model_validate_json(d),
          )],
      }
      ```
      ```
    - Verify: `grep -n "djoutbox\[pydantic\]" docs/publishing.md` returns no matches.

12. **Update worker.md — API table + deserialization table**
    - Files: `docs/worker.md` (edit)
    - Change:
      - In "Worker API" signature (line 119-130), add `serializers: Sequence | None = None,` parameter line.
      - In parameter table (line 132-139), add row: `| serializers | None | Sequence of (type, serializer, deserializer) tuples for custom deserialization |`.
      - Replace "Body deserialization" table (lines 196-204) with:
        ```markdown
        The body type hint determines deserialization:

        | Type hint                                          | Deserialization                                  |
        | -------------------------------------------------- | ------------------------------------------------ |
        | Match in `Worker.serializers` (issubclass check)   | Custom deserializer called as `deserializer(type, body)` |
        | `bytes`                                            | Raw body                                         |
        | `dict` / no hint / other                           | `json.loads()`                                   |

        Custom types are configured on the `Worker` via the `serializers` kwarg. See installation.md "Custom serializers" for a Pydantic example.
        ```
    - Verify: `grep -n "model_validate_json" docs/worker.md` returns no matches outside the new custom-serializers pointer.

13. **Update examples/myproject/settings.py**
    - Files: `examples/myproject/settings.py` (edit)
    - Change: Update `DJOUTBOX` dict (line 37):
      ```python
      from myapp.types import Payload  # or wherever BaseModel subclass lives

      DJOUTBOX = {
          "rmq_url": "amqp://guest:guest@localhost/",
          "serializers": [
              (
                  __import__("pydantic").BaseModel,
                  lambda m: m.model_dump_json().encode(),
                  lambda cls, d: cls.model_validate_json(d),
              ),
          ],
      }
      ```
      Simpler form: `from pydantic import BaseModel` at top, then use `BaseModel` directly in tuple. Don't reference `Payload` — register the abstract `BaseModel` so all subclasses work.
    - Verify: `cd examples && uv run python -c "import os; os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings'); import django; django.setup(); from django.conf import settings; assert settings.DJOUTBOX['serializers']"` succeeds (must run from `examples/` dir with proper venv).

## Parallelization

- Track A steps 1-4 must run in order (each edits a different file but all are core code — do them sequentially to avoid thrash).
- Track B depends on Track A (tests exercise new code paths).
- Track C is independent of Track B. Tracks B and C may run in either order or concurrently.
- Steps 5-9 (tests) and 10-13 (docs/examples) touch disjoint files — safe in parallel.

## Testing strategy

- Run: `uv run pytest --cov --cov-report=term-missing -v` (Makefile `test` target).
- Expect: all existing tests pass, plus the new tests added in steps 8-9. Test count: existing suite + 2 new tests in test_worker.py.
- New tests added:
  - `test_publisher.py::test_serialize_pydantic` (rewritten) — pins publisher registry lookup via `settings.DJOUTBOX["serializers"]`.
  - `test_worker.py::test_consumer_deserializes_via_registry` — pins worker registry path with a fake message.
  - `test_worker.py::test_consumer_bytes_passthrough` — pins bytes regression after registry refactor.
- Edge cases covered:
  - Publisher with no `serializers` setting → falls back to bytes/json (existing tests cover).
  - Publisher with serializer that doesn't match → falls through to bytes/json (existing `test_serialize_dict`/`test_serialize_bytes` cover since they don't register anything).
  - Worker with `serializers=None` → bytes/json fallback (existing test_consumer tests + new bytes test).
  - Worker deserialization raising → existing `try/except` nacks + metrics (no behavior change).

## Success criteria

- [ ] `uv run pytest -v` exits 0 with all tests passing.
- [ ] `uv run ruff check src/` exits 0.
- [ ] `uv run ruff format --check src/` exits 0.
- [ ] `uv run mypy src/djoutbox/ --no-incremental` exits 0.
- [ ] `grep -rn "BaseModel" src/djoutbox/` returns zero matches.
- [ ] `grep -n "pydantic = " pyproject.toml` returns zero matches (only dev-group entry remains).
- [ ] `grep -n "djoutbox\[pydantic\]" docs/` returns zero matches.
- [ ] No files outside the listed steps modified.

## Rules for the implementer

- If a step fails twice, STOP. Report what you tried and the exact error. Do not attempt a third variation.
- Touch only files listed in the steps. If you believe another file needs changes, stop and flag it instead of editing.
- If anything unexpected appears (failing pre-existing tests, type errors outside your steps), stop and report — do not work around it.
- Work step by step in order within each track. Do not batch multiple steps before verifying.
- Do not add a `**_ignored: object` catch-all to `Worker`. Users with extra DJOUTBOX keys still get TypeError — same as today.
- Do not invent a module-level registry. All lookups happen at point of use: publisher reads `settings.DJOUTBOX` per call; worker reads `self.serializers` passed at construction.
