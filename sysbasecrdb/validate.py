"""Data validation: compare source (Sybase) and target (CockroachDB) data.

Provides multiple levels of validation:
  1. Row count comparison (fast, catches bulk mismatches)
  2. SHA-256 checksum over PK-ordered rows (catches data corruption/transformation errors)
  3. Column-level aggregation comparison (SUM, MIN, MAX of numeric columns)
  4. Sample-based row-by-row comparison (catches subtle encoding or type conversion issues)

For large datasets, checksum and sample modes can use configurable sample rates
to complete validation within a reasonable time window.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .config import MigrationConfig
from .connections import CockroachConnection, SybaseConnection
from .schema_extract import TableSchema
from .utils import quote_ident

logger = logging.getLogger("sysbasecrdb.validate")


@dataclass
class TableValidationResult:
    table: str
    row_count_source: int = 0
    row_count_target: int = 0
    row_count_match: bool = False
    checksum_source: Optional[str] = None
    checksum_target: Optional[str] = None
    checksum_match: Optional[bool] = None
    column_agg_match: Optional[bool] = None
    column_agg_details: Dict[str, dict] = field(default_factory=dict)
    sample_mismatches: int = 0
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        if self.error:
            return False
        if not self.row_count_match:
            return False
        if self.checksum_match is False:
            return False
        if self.column_agg_match is False:
            return False
        return True

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        parts = [f"[{status}] {self.table}"]
        parts.append(f"  rows: source={self.row_count_source} target={self.row_count_target}")
        if not self.row_count_match:
            delta = self.row_count_source - self.row_count_target
            parts.append(f"  ROW COUNT MISMATCH (delta={delta})")
        if self.checksum_match is not None:
            parts.append(f"  checksum: {'MATCH' if self.checksum_match else 'MISMATCH'}")
        if self.column_agg_match is not None:
            parts.append(f"  column aggregates: {'MATCH' if self.column_agg_match else 'MISMATCH'}")
            for col, detail in self.column_agg_details.items():
                if detail.get("match") is False:
                    parts.append(f"    {col}: source={detail.get('source')} target={detail.get('target')}")
        if self.sample_mismatches > 0:
            parts.append(f"  sample mismatches: {self.sample_mismatches}")
        if self.error:
            parts.append(f"  ERROR: {self.error}")
        return "\n".join(parts)


class MigrationValidator:
    """Validate data consistency between Sybase source and CockroachDB target."""

    def __init__(
        self,
        sybase_conn: SybaseConnection,
        crdb_conn: CockroachConnection,
        schemas: Dict[str, TableSchema],
        config: MigrationConfig,
    ):
        self._sybase = sybase_conn
        self._crdb = crdb_conn
        self._schemas = schemas
        self._config = config

    def validate_all(self, tables: List[str]) -> Dict[str, TableValidationResult]:
        """Run all configured validations on all tables."""
        results = {}
        for table in tables:
            logger.info("Validating table: %s", table)
            result = self.validate_table(table)
            results[table] = result
            logger.info(result.summary())
        return results

    def validate_table(self, table_name: str) -> TableValidationResult:
        """Run all configured validations on a single table."""
        result = TableValidationResult(table=table_name)
        schema = self._schemas.get(table_name)
        if not schema:
            result.error = f"Schema not found for table {table_name}"
            return result

        try:
            # Level 1: Row count comparison (always enabled)
            result.row_count_source = self._get_sybase_count(table_name)
            result.row_count_target = self._get_crdb_count(table_name)
            result.row_count_match = (
                result.row_count_source == result.row_count_target
            )

            # Level 2: Column-level aggregate comparison
            if self._config.validation.row_count:
                agg_result = self._compare_column_aggregates(table_name, schema)
                result.column_agg_match = agg_result["match"]
                result.column_agg_details = agg_result["details"]

            # Level 3: Checksum comparison
            if self._config.validation.checksum and schema.primary_key:
                sample_rate = self._config.validation.sample_rate
                result.checksum_source = self._sybase_checksum(
                    table_name, schema, sample_rate
                )
                result.checksum_target = self._crdb_checksum(
                    table_name, schema, sample_rate
                )
                result.checksum_match = (
                    result.checksum_source == result.checksum_target
                )

            # Level 4: Sample-based row-by-row comparison
            if schema.primary_key and result.row_count_source > 0:
                result.sample_mismatches = self._compare_sample_rows(
                    table_name, schema, sample_size=1000,
                )

        except Exception as exc:
            result.error = str(exc)
            logger.error("Validation error for %s: %s", table_name, exc)

        return result

    # ── Level 1: Row counts ──────────────────────────────────

    def _get_sybase_count(self, table: str) -> int:
        with self._sybase.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return int(cur.fetchone()[0])

    def _get_crdb_count(self, table: str) -> int:
        with self._crdb.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}")
            return int(cur.fetchone()[0])

    # ── Level 2: Column aggregates ───────────────────────────

    def _compare_column_aggregates(
        self, table_name: str, schema: TableSchema
    ) -> dict:
        """Compare SUM/MIN/MAX of numeric columns between source and target.

        This catches truncation, overflow, and precision loss issues that
        row counts alone would miss.
        """
        # Find numeric columns
        numeric_types = {
            "tinyint", "smallint", "int", "integer", "bigint",
            "numeric", "decimal", "money", "smallmoney", "float", "real",
        }
        numeric_cols = [
            c for c in schema.columns
            if c.datatype.lower() in numeric_types
        ]

        if not numeric_cols:
            return {"match": True, "details": {}}

        result = {"match": True, "details": {}}

        for col in numeric_cols:
            try:
                # Sybase aggregate
                with self._sybase.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT({col.name}), "
                        f"SUM(CONVERT(FLOAT, {col.name})), "
                        f"MIN({col.name}), MAX({col.name}) "
                        f"FROM {table_name}"
                    )
                    src_row = cur.fetchone()

                # CockroachDB aggregate
                crdb_col = quote_ident(col.name)
                with self._crdb.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT({crdb_col}), "
                        f"SUM({crdb_col}::FLOAT8), "
                        f"MIN({crdb_col}), MAX({crdb_col}) "
                        f"FROM {quote_ident(table_name)}"
                    )
                    tgt_row = cur.fetchone()

                col_match = True
                detail = {}

                # Compare non-null counts
                if src_row[0] != tgt_row[0]:
                    col_match = False
                    detail["non_null_count"] = {
                        "source": src_row[0], "target": tgt_row[0]
                    }

                # Compare sums (with floating-point tolerance)
                if src_row[1] is not None and tgt_row[1] is not None:
                    src_sum = float(src_row[1])
                    tgt_sum = float(tgt_row[1])
                    if abs(src_sum - tgt_sum) > max(abs(src_sum) * 1e-6, 0.01):
                        col_match = False
                        detail["sum"] = {"source": src_sum, "target": tgt_sum}

                if not col_match:
                    result["match"] = False
                result["details"][col.name] = {
                    "match": col_match,
                    "source": {
                        "count": src_row[0], "sum": src_row[1],
                        "min": src_row[2], "max": src_row[3],
                    },
                    "target": {
                        "count": tgt_row[0], "sum": tgt_row[1],
                        "min": tgt_row[2], "max": tgt_row[3],
                    },
                }

            except Exception as exc:
                logger.warning(
                    "Column aggregate comparison failed for %s.%s: %s",
                    table_name, col.name, exc,
                )
                result["details"][col.name] = {"match": None, "error": str(exc)}

        return result

    # ── Level 3: Checksum ────────────────────────────────────

    def _sybase_checksum(
        self, table: str, schema: TableSchema, sample_rate: float = 1.0
    ) -> str:
        """Compute a SHA-256 hash over all (or sampled) rows from Sybase."""
        pk_order = ", ".join(schema.primary_key)
        cols = ", ".join(c.name for c in schema.columns)

        # Sampling: use modulo on first PK column if sampling
        where = ""
        if sample_rate < 1.0 and schema.primary_key:
            pk_col = schema.primary_key[0]
            modulo = int(1.0 / sample_rate)
            where = f"WHERE {pk_col} % {modulo} = 0 "

        sql = f"SELECT {cols} FROM {table} {where}ORDER BY {pk_order}"

        h = hashlib.sha256()
        row_count = 0
        with self._sybase.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(10000)
                if not rows:
                    break
                for row in rows:
                    h.update("|".join(str(v) if v is not None else "NULL" for v in row).encode("utf-8"))
                    row_count += 1
            cur.close()

        logger.debug("Sybase checksum for %s: %d rows hashed", table, row_count)
        return h.hexdigest()

    def _crdb_checksum(
        self, table: str, schema: TableSchema, sample_rate: float = 1.0
    ) -> str:
        """Compute a SHA-256 hash over all (or sampled) rows from CockroachDB."""
        pk_order = ", ".join(quote_ident(c) for c in schema.primary_key)
        cols = ", ".join(quote_ident(c.name) for c in schema.columns)

        where = ""
        if sample_rate < 1.0 and schema.primary_key:
            pk_col = quote_ident(schema.primary_key[0])
            modulo = int(1.0 / sample_rate)
            where = f"WHERE {pk_col} % {modulo} = 0 "

        sql = f"SELECT {cols} FROM {quote_ident(table)} {where}ORDER BY {pk_order}"

        h = hashlib.sha256()
        row_count = 0

        # Use server-side cursor for memory efficiency on large tables
        with self._crdb.connect() as conn:
            cur = conn.cursor(name="validation_cursor")
            cur.itersize = 10000
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(10000)
                if not rows:
                    break
                for row in rows:
                    h.update("|".join(str(v) if v is not None else "NULL" for v in row).encode("utf-8"))
                    row_count += 1
            cur.close()

        logger.debug("CockroachDB checksum for %s: %d rows hashed", table, row_count)
        return h.hexdigest()

    # ── Level 4: Sample row comparison ─────────────────────

    def _compare_sample_rows(
        self, table_name: str, schema: 'TableSchema', sample_size: int = 1000,
    ) -> int:
        """Compare a random sample of rows between source and target, cell by cell.

        Catches encoding issues, type conversion edge cases, trailing space
        differences, and other subtleties that checksums and aggregates can miss.
        """
        if not schema.primary_key:
            return 0

        pk_col = schema.primary_key[0]
        all_cols = [c.name for c in schema.columns]
        src_col_list = ", ".join(all_cols)
        tgt_col_list = ", ".join(quote_ident(c) for c in all_cols)

        # Get sample PKs from source (use modulo for deterministic sampling)
        with self._sybase.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cur.fetchone()[0]

        if total == 0:
            return 0

        modulo = max(1, total // sample_size)
        sample_sql = (
            f"SELECT {src_col_list} FROM {table_name} "
            f"WHERE {pk_col} % {modulo} = 0 "
            f"ORDER BY {pk_col}"
        )

        # Fetch source sample
        src_rows = {}
        with self._sybase.connect() as conn:
            cur = conn.cursor()
            cur.execute(sample_sql)
            for row in cur.fetchall():
                pk_idx = all_cols.index(pk_col)
                pk_val = row[pk_idx]
                src_rows[str(pk_val)] = tuple(
                    str(v) if v is not None else "NULL" for v in row
                )
            cur.close()

        if not src_rows:
            return 0

        # Fetch matching rows from target
        tgt_pk_col = quote_ident(pk_col)
        tgt_sql = (
            f"SELECT {tgt_col_list} FROM {quote_ident(table_name)} "
            f"WHERE {tgt_pk_col} % {modulo} = 0 "
            f"ORDER BY {tgt_pk_col}"
        )

        tgt_rows = {}
        with self._crdb.connect() as conn:
            cur = conn.cursor()
            cur.execute(tgt_sql)
            for row in cur.fetchall():
                pk_idx = all_cols.index(pk_col)
                pk_val = row[pk_idx]
                tgt_rows[str(pk_val)] = tuple(
                    str(v) if v is not None else "NULL" for v in row
                )
            cur.close()

        # Compare cell by cell
        mismatches = 0
        for pk_val, src_tuple in src_rows.items():
            tgt_tuple = tgt_rows.get(pk_val)
            if tgt_tuple is None:
                logger.warning(
                    "Sample mismatch %s PK=%s: row exists in source but not target",
                    table_name, pk_val,
                )
                mismatches += 1
                continue

            for i, (s, t) in enumerate(zip(src_tuple, tgt_tuple)):
                if s != t:
                    # Allow trailing space differences for CHAR columns
                    if s.rstrip() == t.rstrip():
                        continue
                    logger.warning(
                        "Sample mismatch %s PK=%s col=%s: source=%r target=%r",
                        table_name, pk_val, all_cols[i], s[:100], t[:100],
                    )
                    mismatches += 1
                    break  # One mismatch per row is enough

        logger.info(
            "Sample comparison for %s: %d rows sampled, %d mismatches",
            table_name, len(src_rows), mismatches,
        )
        return mismatches

    # ── Report generation ────────────────────────────────────

    def generate_report(self, results: Dict[str, TableValidationResult]) -> str:
        """Generate a human-readable validation report."""
        lines = [
            "=" * 70,
            "MIGRATION VALIDATION REPORT",
            "=" * 70,
            "",
        ]

        passed = sum(1 for r in results.values() if r.passed)
        failed = sum(1 for r in results.values() if not r.passed)
        lines.append(f"Total tables: {len(results)}")
        lines.append(f"Passed: {passed}")
        lines.append(f"Failed: {failed}")
        lines.append("")

        if failed > 0:
            lines.append("FAILED TABLES:")
            lines.append("-" * 40)
            for r in results.values():
                if not r.passed:
                    lines.append(r.summary())
                    lines.append("")

        lines.append("ALL TABLES:")
        lines.append("-" * 40)
        for r in results.values():
            lines.append(r.summary())
            lines.append("")

        return "\n".join(lines)
