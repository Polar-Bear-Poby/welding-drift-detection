from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor


def _load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file()


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "15432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "welding_drift")
    postgres_user: str = os.getenv("POSTGRES_USER", "welding")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "welding_local_pw")


settings = Settings()


def get_db() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        cursor_factory=RealDictCursor,
    )
    try:
        yield conn
    finally:
        conn.close()

