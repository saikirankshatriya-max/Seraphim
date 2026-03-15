"""Replay CDC log entries from Sybase to CockroachDB with idempotent operations.

Reads batches from per-table _cdc_log_<table> tables on Sybase and applies them
to CockroachDB using:
  - INSERT ON CONFLICT DO NOTHING (for inserts)
  - UPSERT (for updates)
  - DELETE WHERE PK (for deletes)

All operations are safe to replay from any checkpoint.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from .config import MigrationConfig
from .connections import CockroachConnection, SybaseConnection
from .schema_extract import TableSchema
from .state import MigrationState
from .utils import quote_ident

logger = logging.getLogger("sysbasecrdb.cdc_replay")


class CDCReplayer:
    """Poll Sybase CDC log tables and replay changes to CockroachDB."""

    def __init__(
        self,
        config: MigrationConfig,
        state: MigrationState,
        sybase_conn: SybaseConnection,
        crdb_conn: CockroachConnection,
        schemas: Dict[str, TableSchema],
    ):
        self._config = config
        self._state = state
        self._sybase = sybase_conn
        self._crdb = crdb_conn
        self._schemas = schemas

    def replay_loop(self, tables: List[str], until_cutover: bool = True) -> None:
        """Main CDC replay loop.

        Runs until all tables have lag below the configured threshold.
        If until_cutover is False, runs a single pass instead.
        """
        iteration = 0
        while True:
            iteration += 1
            lags: Dict[str, int] = {}

            with ThreadPoolExecutor(max_workers=self._config.cdc_replay_workers) as pool:
                futures = {
                    pool.submit(self._replay_batch, table): table
                    for table in tables
                }
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        remaining = future.result()
                        lags[table] = remaining
                    except Exception as exc:
                        logger.error("CDC replay error for %s: %s", table, exc)
                        lags[table] = -1  # Signal error but continue

            # Log progress
            total_lag = sum(v for v in lags.values() if v >= 0)
            error_count = sum(1 for v in lags.values() if v < 0)
            logger.info(
                "CDC replay iteration %d: total_lag=%d across %d tables (%d errors)",
                iteration, total_lag, len(lags), error_count,
            )

            if not until_cutover:
                break

            # Check cutover condition
            max_lag = max((v for v in lags.values() if v >= 0), default=0)
            if max_lag <= self._config.cdc.lag_threshold:
                logger.info(
                    "CDC caught up: max_lag=%d <= threshold=%d — ready for cutover",
                    max_lag, self._config.cdc.lag_threshold,
                )
                break

            time.sleep(self._config.cdc.poll_interval_seconds)

    def _replay_batch(self, table_name: str) -> int:
        """Read a batch from _cdc_log_<table> and apply to CockroachDB.

        Returns the estimated remaining rows in the CDC log after this batch.
        """
        schema = self._schemas[table_name]
        pk_cols = schema.primary_key or []
        all_cols = [c.name for c in schema.columns]

        if not pk_cols:
            logger.warning(
                "Table %s has no primary key — CDC replay will use INSERT only", table_name
            )

        last_seq = self._state.get_last_cdc_seq(table_name) or 0

        # Read batch from Sybase CDC log
        col_list = ", ".join(all_cols)
        select_sql = (
            f"SELECT TOP {self._config.cdc.batch_size} "
            f"cdc_seq, cdc_operation, {col_list} "
            f"FROM _cdc_log_{table_name} "
            f"WHERE cdc_seq > {last_seq} "
            f"ORDER BY cdc_seq"
        )

        with self._sybase.cursor() as cur:
            cur.execute(select_sql)
            rows = cur.fetchall()

        if not rows:
            return 0

        # Apply to CockroachDB
        applied = 0
        with self._crdb.connect() as conn:
            cur = conn.cursor()
            try:
                for row in rows:
                    seq = row[0]
                    op = row[1].strip().upper()
                    values = tuple(row[2:])

                    if op == "I":
                        self._apply_insert(cur, table_name, all_cols, pk_cols, values)
                    elif op == "U":
                        self._apply_upsert(cur, table_name, all_cols, pk_cols, values)
                    elif op == "D":
                        self._apply_delete(cur, table_name, all_cols, pk_cols, values)
                    else:
                        logger.warning("Unknown CDC operation '%s' at seq %d", op, seq)

                    last_seq = seq
                    applied += 1

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

        self._state.set_last_cdc_seq(table_name, last_seq)
        logger.debug("Table %s: applied %d CDC entries (last_seq=%d)", table_name, applied, last_seq)

        # Estimate remaining
        remaining = self._get_remaining_count(table_name, last_seq)
        return remaining

    def _get_remaining_count(self, table_name: str, after_seq: int) -> int:
        """Count remaining CDC log entries after a given sequence."""
        sql = f"SELECT COUNT(*) FROM _cdc_log_{table_name} WHERE cdc_seq > {after_seq}"
        with self._sybase.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    # ── idempotent DML operations ────────────────────────────

    def _apply_insert(
        self, cur, table_name: str, all_cols: List[str],
        pk_cols: List[str], values: tuple
    ) -> None:
        """INSERT ON CONFLICT DO NOTHING (idempotent)."""
        col_list = ", ".join(quote_ident(c) for c in all_cols)
        placeholders = ", ".join(["%s"] * len(all_cols))

        if pk_cols:
            pk_list = ", ".join(quote_ident(c) for c in pk_cols)
            sql = (
                f"INSERT INTO {quote_ident(table_name)} ({col_list}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO NOTHING"
            )
        else:
            sql = (
                f"INSERT INTO {quote_ident(table_name)} ({col_list}) "
                f"VALUES ({placeholders})"
            )
        cur.execute(sql, values)

    def _apply_upsert(
        self, cur, table_name: str, all_cols: List[str],
        pk_cols: List[str], values: tuple
    ) -> None:
        """UPSERT (INSERT ON CONFLICT DO UPDATE SET all non-PK columns)."""
        col_list = ", ".join(quote_ident(c) for c in all_cols)
        placeholders = ", ".join(["%s"] * len(all_cols))

        sql = (
            f"UPSERT INTO {quote_ident(table_name)} ({col_list}) "
            f"VALUES ({placeholders})"
        )
        cur.execute(sql, values)

    def _apply_delete(
        self, cur, table_name: str, all_cols: List[str],
        pk_cols: List[str], values: tuple
    ) -> None:
        """DELETE by PK values."""
        if not pk_cols:
            logger.warning("Cannot apply DELETE for %s — no primary key", table_name)
            return

        # Extract PK values from the full row
        pk_indices = [all_cols.index(pk) for pk in pk_cols]
        pk_values = tuple(values[i] for i in pk_indices)
        where_clause = " AND ".join(
            f"{quote_ident(pk)} = %s" for pk in pk_cols
        )
        sql = f"DELETE FROM {quote_ident(table_name)} WHERE {where_clause}"
        cur.execute(sql, pk_values)
