"""Parallel data export from Sybase ASE using BCP with PK-range chunking.

Large tables (>threshold rows) are split into PK-range chunks for parallel export.
Small tables are exported as a single file.
Tables with TEXT/IMAGE columns fall back to pyodbc streaming.
"""

import logging
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from .config import MigrationConfig
from .connections import SybaseConnection
from .schema_extract import TableSchema
from .state import MigrationState
from .utils import escape_csv_value

logger = logging.getLogger("sysbasecrdb.export")

# Sybase types that BCP cannot handle well in character mode
_LOB_TYPES = {"text", "unitext", "image"}


class DataExporter:
    """Export Sybase tables to CSV files using BCP or pyodbc streaming."""

    def __init__(
        self,
        config: MigrationConfig,
        state: MigrationState,
        sybase_conn: SybaseConnection,
        schemas: Dict[str, TableSchema],
    ):
        self._config = config
        self._state = state
        self._conn = sybase_conn
        self._schemas = schemas

    def export_all(self, tables: List[str]) -> None:
        """Export all tables in parallel using a process pool."""
        logger.info("Starting parallel export of %d tables (workers=%d)",
                     len(tables), self._config.export_workers)

        # We use ThreadPoolExecutor here because each worker spawns BCP subprocesses
        # (ProcessPoolExecutor would be for CPU-bound Python work)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self._config.export_workers) as pool:
            futures = {}
            for table in tables:
                status = self._state.get_table_status(table)
                if status in ("exported", "importing", "imported",
                              "cdc_replaying", "validated", "complete"):
                    logger.info("Skipping already-exported table: %s (status=%s)", table, status)
                    continue
                futures[pool.submit(self._export_table, table)] = table

            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                    self._state.set_table_status(table, "exported")
                    logger.info("Export complete: %s", table)
                except Exception as exc:
                    self._state.set_table_error(table, f"Export failed: {exc}")
                    logger.error("Export failed for %s: %s", table, exc)

    def _export_table(self, table_name: str) -> None:
        """Export one table — chunked if large, single BCP otherwise."""
        schema = self._schemas[table_name]
        self._state.set_table_status(table_name, "exporting")

        export_dir = os.path.join(self._config.export_dir, table_name)
        os.makedirs(export_dir, exist_ok=True)

        # Check if table has LOB columns requiring special handling
        has_lobs = any(c.datatype.lower() in _LOB_TYPES for c in schema.columns)

        if has_lobs:
            logger.info("Table %s has LOB columns — using pyodbc streaming", table_name)
            self._export_via_odbc(table_name, schema, export_dir)
        elif (schema.row_count_estimate > self._config.large_table_threshold
              and schema.primary_key
              and len(schema.primary_key) == 1):
            logger.info(
                "Table %s is large (~%d rows) — chunking by PK",
                table_name, schema.row_count_estimate,
            )
            self._export_chunked(table_name, schema, export_dir)
        else:
            self._export_single(table_name, export_dir)

    # ── BCP single-file export ──────────────────────────────

    def _export_single(self, table_name: str, export_dir: str) -> None:
        """Export a table as a single CSV file via BCP."""
        output_file = os.path.join(export_dir, f"{table_name}.csv")
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            logger.info("Chunk file already exists, skipping: %s", output_file)
            return
        self._run_bcp_out(table_name, output_file)

    # ── BCP chunked export ──────────────────────────────────

    def _export_chunked(
        self, table_name: str, schema: TableSchema, export_dir: str
    ) -> None:
        """Export a large table in PK-range chunks via BCP queryout."""
        pk_col = schema.primary_key[0]
        min_pk, max_pk = self._get_pk_range(table_name, pk_col)

        if min_pk is None or max_pk is None:
            logger.warning("Table %s PK range is NULL — falling back to single export", table_name)
            self._export_single(table_name, export_dir)
            return

        boundaries = self._compute_chunk_boundaries(min_pk, max_pk)
        logger.info("Table %s: %d chunks (PK %s range %s..%s)",
                     table_name, len(boundaries), pk_col, min_pk, max_pk)

        for i, (start, end) in enumerate(boundaries):
            chunk_file = os.path.join(export_dir, f"{table_name}_chunk_{i:05d}.csv")
            if os.path.exists(chunk_file) and os.path.getsize(chunk_file) > 0:
                logger.debug("Chunk already exported, skipping: %s", chunk_file)
                continue

            # For the last chunk, use <= to include the max value
            if i == len(boundaries) - 1:
                where = f"{pk_col} >= {start} AND {pk_col} <= {end}"
            else:
                where = f"{pk_col} >= {start} AND {pk_col} < {end}"

            db = self._config.source.database
            query = f"SELECT * FROM {db}..{table_name} WHERE {where}"
            self._run_bcp_queryout(query, chunk_file)

    def _get_pk_range(self, table_name: str, pk_col: str) -> Tuple[Optional[int], Optional[int]]:
        """Query MIN/MAX of the PK column."""
        sql = f"SELECT MIN({pk_col}), MAX({pk_col}) FROM {table_name}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                return row[0], row[1]
            return None, None

    def _compute_chunk_boundaries(self, min_pk: int, max_pk: int) -> List[Tuple[int, int]]:
        """Compute PK range boundaries for chunked export."""
        chunk_size = self._config.chunk_size
        boundaries = []
        current = min_pk
        while current <= max_pk:
            end = current + chunk_size
            if end > max_pk:
                end = max_pk
            boundaries.append((current, end))
            current = end
            if current == max_pk:
                break
        return boundaries

    # ── BCP command execution ────────────────────────────────

    def _run_bcp_out(self, table_name: str, output_file: str) -> None:
        """Run: bcp <db>..<table> out <file> -c -t'|' ..."""
        cfg = self._config.source
        cmd = [
            cfg.bcp_path,
            f"{cfg.database}..{table_name}",
            "out", output_file,
            "-c",
            f"-t{self._config.export.delimiter}",
            f"-S{cfg.host}:{cfg.port}",
            f"-U{cfg.username}",
            f"-P{cfg.password}",
            f"-A{self._config.export.bcp_packet_size}",
        ]
        self._run_subprocess(cmd, f"BCP out {table_name}")

    def _run_bcp_queryout(self, query: str, output_file: str) -> None:
        """Run: bcp '<query>' queryout <file> -c -t'|' ..."""
        cfg = self._config.source
        cmd = [
            cfg.bcp_path,
            query,
            "queryout", output_file,
            "-c",
            f"-t{self._config.export.delimiter}",
            f"-S{cfg.host}:{cfg.port}",
            f"-U{cfg.username}",
            f"-P{cfg.password}",
            f"-A{self._config.export.bcp_packet_size}",
        ]
        self._run_subprocess(cmd, "BCP queryout")

    def _run_subprocess(self, cmd: List[str], description: str) -> None:
        """Execute a subprocess, log output, raise on failure."""
        # Mask password in logged command
        safe_cmd = []
        mask_next = False
        for arg in cmd:
            if mask_next or arg.startswith("-P"):
                safe_cmd.append("-P****")
                mask_next = False
            else:
                safe_cmd.append(arg)
        logger.debug("Running: %s", " ".join(safe_cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hour timeout per chunk/table
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{description} failed (rc={result.returncode}): {result.stderr.strip()}"
            )
        if result.stdout.strip():
            logger.debug("%s output: %s", description, result.stdout.strip()[-500:])

    # ── ODBC streaming fallback ──────────────────────────────

    def _export_via_odbc(
        self, table_name: str, schema: TableSchema, export_dir: str
    ) -> None:
        """Export a table via pyodbc for tables with LOB columns.

        Streams rows and writes CSV manually with proper escaping.
        """
        output_file = os.path.join(export_dir, f"{table_name}.csv")
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            logger.info("ODBC export file already exists, skipping: %s", output_file)
            return

        delimiter = self._config.export.delimiter
        null_marker = self._config.export.null_marker
        col_names = ", ".join(c.name for c in schema.columns)

        # Order by PK for deterministic output
        order_by = ""
        if schema.primary_key:
            order_by = " ORDER BY " + ", ".join(schema.primary_key)

        sql = f"SELECT {col_names} FROM {table_name}{order_by}"

        with self._conn.connect() as conn:
            # Set textsize large enough for LOB columns
            cur = conn.cursor()
            cur.execute("SET TEXTSIZE 2147483647")

            cur.execute(sql)
            with open(output_file, "w", encoding="utf-8") as fh:
                row_count = 0
                while True:
                    rows = cur.fetchmany(10000)
                    if not rows:
                        break
                    for row in rows:
                        values = [
                            escape_csv_value(v, delimiter, null_marker)
                            for v in row
                        ]
                        fh.write(delimiter.join(values) + "\n")
                        row_count += 1

            cur.close()
        logger.info("ODBC export complete for %s: %d rows", table_name, row_count)
