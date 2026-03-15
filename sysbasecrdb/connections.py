"""Connection factories for Sybase ASE (pyodbc) and CockroachDB (psycopg2)."""

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
import pyodbc

from .config import SourceConfig, TargetConfig

logger = logging.getLogger("sysbasecrdb.connections")


class SybaseConnection:
    """Manages pyodbc connections to Sybase ASE."""

    def __init__(self, config: SourceConfig):
        self._config = config

    @contextmanager
    def connect(self) -> Generator[pyodbc.Connection, None, None]:
        """Yield a pyodbc connection; auto-close on exit."""
        cfg = self._config
        if cfg.odbc_dsn:
            conn_str = (
                f"DSN={cfg.odbc_dsn};"
                f"UID={cfg.username};"
                f"PWD={cfg.password};"
            )
        else:
            conn_str = (
                f"DRIVER={{{cfg.driver}}};"
                f"SERVER={cfg.host};"
                f"PORT={cfg.port};"
                f"DATABASE={cfg.database};"
                f"UID={cfg.username};"
                f"PWD={cfg.password};"
                f"CHARSET={cfg.charset};"
            )
        logger.debug("Connecting to Sybase at %s:%s/%s", cfg.host, cfg.port, cfg.database)
        conn = pyodbc.connect(conn_str, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def cursor(self) -> Generator[pyodbc.Cursor, None, None]:
        """Yield a cursor from a fresh connection."""
        with self.connect() as conn:
            cur = conn.cursor()
            try:
                yield cur
            finally:
                cur.close()


class CockroachConnection:
    """Manages psycopg2 connections to CockroachDB."""

    def __init__(self, config: TargetConfig):
        self._config = config

    @contextmanager
    def connect(self) -> Generator:
        """Yield a psycopg2 connection; auto-close on exit."""
        cfg = self._config
        conn_kwargs = dict(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            user=cfg.username,
            password=cfg.password,
            sslmode=cfg.sslmode,
        )
        if cfg.sslrootcert:
            conn_kwargs["sslrootcert"] = cfg.sslrootcert

        logger.debug("Connecting to CockroachDB at %s:%s/%s", cfg.host, cfg.port, cfg.database)
        conn = psycopg2.connect(**conn_kwargs)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def cursor(self, name: str = None) -> Generator:
        """Yield a cursor. Pass name for server-side cursor (large result sets)."""
        with self.connect() as conn:
            cur = conn.cursor(name=name) if name else conn.cursor()
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
