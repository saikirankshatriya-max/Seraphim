"""Generate and install per-table CDC triggers and log tables on Sybase ASE.

Each source table gets:
  - A _cdc_log_<table> shadow table (IDENTITY-sequenced change log)
  - Three triggers: AFTER INSERT, AFTER UPDATE, AFTER DELETE

The CDC log captures every change from the moment triggers are installed,
enabling zero-downtime migration when combined with a bulk export.
"""

import logging
from typing import Dict, List

from .connections import SybaseConnection
from .schema_extract import ColumnInfo, TableSchema

logger = logging.getLogger("sysbasecrdb.cdc_setup")


class CDCSetup:
    """Install and manage CDC triggers on Sybase ASE source tables."""

    def __init__(self, sybase_conn: SybaseConnection, schemas: Dict[str, TableSchema]):
        self._conn = sybase_conn
        self._schemas = schemas

    # ── public API ───────────────────────────────────────────

    def install_all(self, tables: List[str]) -> Dict[str, bool]:
        """Install CDC log tables + triggers for all specified tables.

        Must be called BEFORE bulk export begins.
        Returns {table_name: success}.
        """
        results: Dict[str, bool] = {}
        for table in tables:
            try:
                self.install_for_table(table)
                results[table] = True
                logger.info("CDC installed for %s", table)
            except Exception as exc:
                logger.error("CDC install failed for %s: %s", table, exc)
                results[table] = False
        return results

    def install_for_table(self, table_name: str) -> None:
        """Create _cdc_log_<table> and three triggers for one table."""
        schema = self._schemas[table_name]

        # Check for existing CDC artifacts
        self._check_existing(table_name)

        with self._conn.connect() as conn:
            cur = conn.cursor()

            # 1. Create CDC log table
            ddl = self._generate_cdc_table_ddl(schema)
            cur.execute(ddl)

            # 2. Create triggers (each in its own batch)
            for op in ("INSERT", "UPDATE", "DELETE"):
                trigger_sql = self._generate_trigger_sql(schema, op)
                cur.execute(trigger_sql)

    def uninstall_all(self, tables: List[str]) -> None:
        """Drop CDC triggers and log tables (cleanup after migration)."""
        with self._conn.connect() as conn:
            cur = conn.cursor()
            for table in tables:
                self._drop_trigger_safe(cur, f"_cdc_trg_{table}_ins")
                self._drop_trigger_safe(cur, f"_cdc_trg_{table}_upd")
                self._drop_trigger_safe(cur, f"_cdc_trg_{table}_del")
                self._drop_table_safe(cur, f"_cdc_log_{table}")
                logger.info("CDC artifacts removed for %s", table)

    def get_max_cdc_seq(self, table_name: str) -> int:
        """Return the current max cdc_seq for a given log table (0 if empty)."""
        sql = f"SELECT MAX(cdc_seq) FROM _cdc_log_{table_name}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def get_cdc_count(self, table_name: str, after_seq: int = 0) -> int:
        """Return the count of CDC log entries after a given sequence."""
        sql = f"SELECT COUNT(*) FROM _cdc_log_{table_name} WHERE cdc_seq > {after_seq}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    # ── DDL generation ───────────────────────────────────────

    def _generate_cdc_table_ddl(self, schema: TableSchema) -> str:
        """Generate CREATE TABLE for the CDC log table.

        All mirrored columns are NULLABLE in the log table because
        DELETE operations only populate PK columns.
        """
        log_table = f"_cdc_log_{schema.name}"
        lines = [
            f"CREATE TABLE {log_table} (",
            "    cdc_seq       NUMERIC(19,0) IDENTITY NOT NULL,",
            "    cdc_operation CHAR(1)       NOT NULL,",
            "    cdc_timestamp DATETIME      NOT NULL DEFAULT getdate(),",
        ]

        for col in schema.columns:
            sybase_type = self._sybase_col_definition(col)
            lines.append(f"    {col.name} {sybase_type} NULL,")

        # Remove trailing comma from last column
        lines[-1] = lines[-1].rstrip(",")
        lines.append(")")
        return "\n".join(lines)

    def _generate_trigger_sql(self, schema: TableSchema, operation: str) -> str:
        """Generate CREATE TRIGGER for one operation (INSERT/UPDATE/DELETE).

        - INSERT/UPDATE triggers read from the 'inserted' pseudo-table.
        - DELETE triggers read from the 'deleted' pseudo-table.
        """
        table = schema.name
        op_char = operation[0]  # I, U, or D
        op_lower = operation.lower()[:3]  # ins, upd, del
        trigger_name = f"_cdc_trg_{table}_{op_lower}"
        log_table = f"_cdc_log_{table}"

        # Source pseudo-table
        pseudo = "inserted" if operation in ("INSERT", "UPDATE") else "deleted"
        alias = "i" if pseudo == "inserted" else "d"

        col_names = [col.name for col in schema.columns]
        col_list = ", ".join(col_names)
        select_list = ", ".join(f"{alias}.{c}" for c in col_names)

        # Sybase ASE trigger syntax
        sql = f"""CREATE TRIGGER {trigger_name} ON {table}
FOR {operation} AS
BEGIN
    INSERT INTO {log_table} (cdc_operation, {col_list})
    SELECT '{op_char}', {select_list}
    FROM {pseudo} {alias}
END"""
        return sql

    # ── helpers ──────────────────────────────────────────────

    def _sybase_col_definition(self, col: ColumnInfo) -> str:
        """Reconstruct a Sybase column type string for the CDC log table."""
        base = col.datatype.lower()

        # Types that need length
        if base in ("char", "varchar", "nchar", "nvarchar", "unichar", "univarchar",
                     "binary", "varbinary"):
            return f"{col.datatype}({col.length})"

        # Types that need precision/scale
        if base in ("numeric", "decimal"):
            prec = col.prec if col.prec is not None else 18
            scale = col.scale if col.scale is not None else 0
            return f"{col.datatype}({prec},{scale})"

        # All other types use bare type name
        return col.datatype

    def _check_existing(self, table_name: str) -> None:
        """Check for existing CDC artifacts and warn."""
        log_table = f"_cdc_log_{table_name}"
        sql = f"SELECT 1 FROM sysobjects WHERE name = '{log_table}' AND type = 'U'"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            if cur.fetchone():
                logger.warning(
                    "CDC log table %s already exists — dropping and recreating",
                    log_table,
                )
                # Drop existing triggers first, then the table
                for suffix in ("ins", "upd", "del"):
                    self._drop_trigger_safe(cur, f"_cdc_trg_{table_name}_{suffix}")
                cur.execute(f"DROP TABLE {log_table}")

        # Check for existing triggers on the source table
        for suffix, event in [("ins", "instrig"), ("upd", "updtrig"), ("del", "deltrig")]:
            trg_name = f"_cdc_trg_{table_name}_{suffix}"
            sql = f"SELECT 1 FROM sysobjects WHERE name = '{trg_name}' AND type = 'TR'"
            with self._conn.cursor() as cur:
                cur.execute(sql)
                if cur.fetchone():
                    logger.warning("Trigger %s already exists — dropping", trg_name)
                    cur.execute(f"DROP TRIGGER {trg_name}")

    def _drop_trigger_safe(self, cur, trigger_name: str) -> None:
        """Drop a trigger if it exists, swallowing errors."""
        try:
            cur.execute(
                f"IF EXISTS (SELECT 1 FROM sysobjects WHERE name = '{trigger_name}' AND type = 'TR') "
                f"DROP TRIGGER {trigger_name}"
            )
        except Exception as exc:
            logger.debug("Could not drop trigger %s: %s", trigger_name, exc)

    def _drop_table_safe(self, cur, table_name: str) -> None:
        """Drop a table if it exists, swallowing errors."""
        try:
            cur.execute(
                f"IF EXISTS (SELECT 1 FROM sysobjects WHERE name = '{table_name}' AND type = 'U') "
                f"DROP TABLE {table_name}"
            )
        except Exception as exc:
            logger.debug("Could not drop table %s: %s", table_name, exc)
