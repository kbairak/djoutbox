import os
from collections.abc import AsyncGenerator, Generator

import django
from django.conf import settings

os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "1"

settings.configure(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "test",
            "USER": "test",
            "PASSWORD": "test",
            "HOST": "localhost",
            "PORT": "5432",
        }
    },
    INSTALLED_APPS=[
        "django.contrib.admin",
        "django.contrib.auth",
        "django.contrib.contenttypes",
        "django.contrib.messages",
        "django.contrib.sessions",
        "djoutbox",
    ],
    DJOUTBOX={},
    USE_TZ=True,
    STORAGES={
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        },
    },
)
django.setup()

import asyncpg  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from aio_pika.abc import AbstractConnection  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402
from testcontainers.rabbitmq import RabbitMqContainer  # noqa: E402


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer]:
    with PostgresContainer("postgres:17-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[RabbitMqContainer]:
    with RabbitMqContainer("rabbitmq:4-alpine") as rmq:
        yield rmq


@pytest.fixture(scope="session")
def db_settings(
    postgres_container: PostgresContainer,
    django_db_blocker,
) -> None:
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(postgres_container.port)
    settings.DATABASES["default"].update(
        HOST=host,
        PORT=port,
        USER=postgres_container.username,
        PASSWORD=postgres_container.password,
        NAME=postgres_container.dbname,
    )
    from django.core.management import call_command

    with django_db_blocker.unblock():
        call_command("migrate", "djoutbox", verbosity=0)


@pytest.fixture(scope="session")
def django_db_setup(
    django_db_blocker,
    db_settings,
):
    return


@pytest_asyncio.fixture
async def db_connection(db_settings) -> AsyncGenerator[asyncpg.Connection]:
    db = settings.DATABASES["default"]
    url = f"postgresql://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:{db['PORT']}/{db['NAME']}"
    conn = await asyncpg.connect(url)
    try:
        from djoutbox.partitions import ensure_partitions

        await ensure_partitions(conn, "1d")
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def worker(rmq_connection: AbstractConnection):
    from djoutbox.worker import Worker as _Worker

    return _Worker(rmq_connection=rmq_connection)


@pytest_asyncio.fixture(scope="session")
async def rmq_connection(
    rabbitmq_container: RabbitMqContainer,
) -> AsyncGenerator[AbstractConnection]:
    import aio_pika

    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(rabbitmq_container.port)
    user = rabbitmq_container.username
    password = rabbitmq_container.password
    url = f"amqp://{user}:{password}@{host}:{port}/"
    connection = await aio_pika.connect(url)
    async with connection:
        yield connection
