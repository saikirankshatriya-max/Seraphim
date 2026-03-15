"""Import CSV data into CockroachDB using IMPORT INTO or COPY FROM.

- IMPORT INTO: used for large files (fastest, distributed ingest, but table goes offline)
- COPY FROM: used for smaller files (stays online, streamed from client)
"""

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from .config import MigrationConfig
from .connections import CockroachConnection
from .schema_extract import TableSchema
from .state import MigrationState
from .utils import quote_ident

logger = logging.getLogger("sysbasecrdb.import_data")


class DataImporter:
    """Import exported CSV files into CockroachDB tables."""

    def __init__(
        self,
        config: MigrationConfig,
        state: MigrationState,
        crdb_conn: CockroachConnection,
        schemas: Dict[str, TableSchema],
    ):
        self._config = config
        self._state = state
        self._crdb = crdb_conn
        self._schemas = schemas

    def import_all(self, tables: List[str]) -> None:
        """Import all exported tables into CockroachDB in parallel."""
        logger.info("Starting parallel import of %d tables (workers=%d)",
                     len(tables), self._config.import_workers)

        with ThreadPoolExecutor(max_workers=self._config.import_workers) as pool:
            futures = {}
            for table in tables:
                status = self._state.get_table_status(table)
                if status != "exported":
                    logger.debug("Skipping %s (status=%s, expected 'exported')", table, status)
                    continue
                futures[pool.submit(self._import_table, table)] = table

            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                    self._state.set_table_status(table, "imported")
                    logger.info("Import complete: %s", table)
                except Exception as exc:
                    self._state.set_table_error(table, f"Import failed: {exc}")
                    logger.error("Import failed for %s: %s", table, exc)

    def _import_table(self, table_name: str) -> None:
        """Import one table, choosing method based on total file size."""
        self._state.set_table_status(table_name, "importing")

        export_dir = os.path.join(self._config.export_dir, table_name)
        if not os.path.isdir(export_dir):
            raise FileNotFoundError(f"Export directory not found: {export_dir}")

        files = sorted(
            f for f in os.listdir(export_dir)
            if f.endswith(".csv") and os.path.getsize(os.path.join(export_dir, f)) > 0
        )
        if not files:
            logger.warning("No CSV files found for %s — skipping", table_name)
            return

        total_size = sum(
            os.path.getsize(os.path.join(export_dir, f)) for f in files
        )
        logger.info(
            "Table %s: %d files, %.2f MB total",
            table_name, len(files), total_size / (1024 * 1024),
        )

        import_method = self._config.target.import_method

        if import_method == "copy_only":
            self._import_via_copy(table_name, export_dir, files)
        elif total_size > self._config.import_cfg.copy_threshold:
            self._import_via_import_into(table_name, export_dir, files)
        else:
            self._import_via_copy(table_name, export_dir, files)

    # ── IMPORT INTO (fastest for large datasets) ─────────────

    def _import_via_import_into(
        self, table_name: str, export_dir: str, files: List[str]
    ) -> None:
        """Upload files and execute IMPORT INTO on CockroachDB."""
        schema = self._schemas[table_name]
        col_names = ", ".join(quote_ident(c.name) for c in schema.columns)
        delimiter = self._config.export.delimiter
        null_marker = self._config.export.null_marker

        method = self._config.target.import_method

        if method == "userfile":
            remote_uris = []
            for f in files:
                local_path = os.path.join(export_dir, f)
                remote_path = f"migration/{table_name}/{f}"
                self._upload_userfile(local_path, remote_path)
                remote_uris.append(f"'userfile:///{remote_path}'")
        elif method == "nodelocal":
            remote_uris = [
                f"'nodelocal://1/migration/{table_name}/{f}'" for f in files
            ]
        else:
            # Fallback to COPY
            logger.warning("Unknown import_method '%s' — falling back to COPY", method)
            self._import_via_copy(table_name, export_dir, files)
            return

        uri_list = ", ".join(remote_uris)
        import_sql = (
            f"IMPORT INTO {quote_ident(table_name)} ({col_names}) "
            f"CSV DATA ({uri_list}) "
            f"WITH delimiter = e'{delimiter}', "
            f"nullif = e'{null_marker}';"
        )

        logger.info("Running IMPORT INTO for %s (%d files)", table_name, len(files))
        with self._crdb.connect() as conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(import_sql)
            result = cur.fetchone()
            if result:
                logger.info("IMPORT INTO %s result: %s", table_name, result)
            cur.close()

    def _upload_userfile(self, local_path: str, remote_path: str) -> None:
        """Upload a file to CockroachDB userfile storage."""
        cfg = self._config.target
        url = (
            f"postgresql://{cfg.username}:{cfg.password}"
            f"@{cfg.host}:{cfg.port}"
            f"/{cfg.database}?sslmode={cfg.sslmode}"
        )
        if cfg.sslrootcert:
            url += f"&sslrootcert={cfg.sslrootcert}"

        cmd = [
            *cfg.userfile_upload_cmd.split(),
            local_path,
            remote_path,
            f"--url={url}",
        ]
        logger.debug("Uploading %s -> userfile:///%s", local_path, remote_path)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode != 0:
            raise RuntimeError(
                f"userfile upload failed for {local_path}: {result.stderr.strip()}"
            )

    # ── COPY FROM (for smaller files or fallback) ────────────

    def _import_via_copy(
        self, table_name: str, export_dir: str, files: List[str]
    ) -> None:
        """Stream data into CockroachDB using COPY FROM STDIN via psycopg2."""
        schema = self._schemas[table_name]
        col_names = ", ".join(quote_ident(c.name) for c in schema.columns)
        delimiter = self._config.export.delimiter
        null_marker = self._config.export.null_marker

        logger.info("Running COPY FROM for %s (%d files)", table_name, len(files))

        with self._crdb.connect() as conn:
            cur = conn.cursor()
            for f in files:
                filepath = os.path.join(export_dir, f)
                logger.debug("COPY FROM file: %s", filepath)
                with open(filepath, "r", encoding="utf-8") as fh:
                    copy_sql = (
                        f"COPY {quote_ident(table_name)} ({col_names}) "
                        f"FROM STDIN WITH ("
                        f"DELIMITER '{delimiter}', "
                        f"NULL '{null_marker}'"
                        f")"
                    )
                    cur.copy_expert(copy_sql, fh)
            conn.commit()
            cur.close()
