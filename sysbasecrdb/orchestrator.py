"""End-to-end migration pipeline as a resumable state machine.

Sequences all migration phases:
  INIT -> schema extraction -> CDC install -> schema creation ->
  export -> import -> CDC replay -> validate -> FK creation -> cleanup -> COMPLETE

Supports resuming from any phase via the state file.
"""

import json
import logging
import os
from typing import Dict, List, Optional

from .cdc_replay import CDCReplayer
from .cdc_setup import CDCSetup
from .config import MigrationConfig
from .connections import CockroachConnection, SybaseConnection
from .export import DataExporter
from .import_data import DataImporter
from .schema_convert import SchemaConverter
from .schema_extract import SchemaExtractor, TableSchema
from .state import MigrationPhase, MigrationState
from .validate import MigrationValidator

logger = logging.getLogger("sysbasecrdb.orchestrator")


class MigrationOrchestrator:
    """Runs the full migration pipeline as a resumable state machine."""

    def __init__(self, config: MigrationConfig):
        self._config = config
        self._state = MigrationState(config.state_file)
        self._sybase = SybaseConnection(config.source)
        self._crdb = CockroachConnection(config.target)
        self._schemas: Dict[str, TableSchema] = {}
        self._tables: List[str] = []

    def run(self, start_from: Optional[str] = None) -> None:
        """Execute migration from current phase (or override with start_from)."""
        phase = MigrationPhase(start_from) if start_from else self._state.get_phase()

        pipeline = [
            (MigrationPhase.INIT, self._step_extract_schema),
            (MigrationPhase.SCHEMA_EXTRACTED, self._step_install_cdc),
            (MigrationPhase.CDC_INSTALLED, self._step_create_schema),
            (MigrationPhase.SCHEMA_CREATED, self._step_export),
            (MigrationPhase.EXPORT_DONE, self._step_import),
            (MigrationPhase.IMPORT_DONE, self._step_cdc_replay),
            (MigrationPhase.CDC_REPLAYING, self._step_validate),
            (MigrationPhase.VALIDATED, self._step_apply_fks),
            (MigrationPhase.FKS_APPLIED, self._step_cleanup),
            (MigrationPhase.CLEANUP, self._step_complete),
        ]

        # For any phase after INIT, we need schemas loaded
        if phase != MigrationPhase.INIT:
            self._load_cached_schemas()

        # Find starting point and execute from there
        executing = False
        for step_phase, step_func in pipeline:
            if step_phase == phase:
                executing = True
            if executing:
                logger.info("=" * 60)
                logger.info("PHASE: %s", step_phase.value)
                logger.info("=" * 60)
                try:
                    step_func()
                except Exception as exc:
                    logger.error("Phase %s failed: %s", step_phase.value, exc)
                    raise

    # ── individual steps ─────────────────────────────────────

    def _step_extract_schema(self) -> None:
        """Extract schema from Sybase and cache it."""
        extractor = SchemaExtractor(self._sybase, self._config.source.database)
        self._schemas = extractor.get_all_schemas(
            self._config.table_include, self._config.table_exclude
        )
        self._tables = list(self._schemas.keys())

        # Initialize per-table state
        for table in self._tables:
            self._state.init_table(table)

        # Cache schemas to disk for resume
        self._save_cached_schemas()

        logger.info("Extracted schemas for %d tables", len(self._schemas))
        self._state.set_phase(MigrationPhase.SCHEMA_EXTRACTED)

    def _step_install_cdc(self) -> None:
        """Install CDC triggers on all source tables."""
        cdc = CDCSetup(self._sybase, self._schemas)
        results = cdc.install_all(self._tables)

        failed = [t for t, ok in results.items() if not ok]
        if failed:
            logger.error("CDC install failed for %d tables: %s", len(failed), failed)
            for t in failed:
                self._state.set_table_error(t, "CDC trigger installation failed")

        self._state.set_phase(MigrationPhase.CDC_INSTALLED)

    def _step_create_schema(self) -> None:
        """Create tables and indexes on CockroachDB (no FKs yet)."""
        converter = SchemaConverter()

        with self._crdb.connect() as conn:
            cur = conn.cursor()
            for table_name, schema in self._schemas.items():
                try:
                    ddl = converter.generate_create_table(schema, include_fks=False)
                    logger.debug("Creating table: %s", table_name)
                    cur.execute(ddl)

                    for idx_sql in converter.generate_indexes(schema):
                        cur.execute(idx_sql)
                except Exception as exc:
                    logger.error("Schema creation failed for %s: %s", table_name, exc)
                    self._state.set_table_error(table_name, f"DDL failed: {exc}")
            conn.commit()
            cur.close()

        # Also save the full DDL to a file for reference
        ddl_path = os.path.join(self._config.export_dir, "_schema.sql")
        os.makedirs(self._config.export_dir, exist_ok=True)
        with open(ddl_path, "w") as fh:
            fh.write(converter.generate_full_ddl(self._schemas))
        logger.info("DDL saved to %s", ddl_path)

        self._state.set_phase(MigrationPhase.SCHEMA_CREATED)

    def _step_export(self) -> None:
        """Export data from Sybase using parallel BCP."""
        self._state.set_phase(MigrationPhase.EXPORTING)

        exporter = DataExporter(self._config, self._state, self._sybase, self._schemas)
        exporter.export_all(self._tables)

        # Check for errors
        errors = self._state.get_error_tables()
        if errors:
            logger.warning(
                "%d tables had export errors — continuing with successfully exported tables",
                len(errors),
            )

        self._state.set_phase(MigrationPhase.EXPORT_DONE)

    def _step_import(self) -> None:
        """Import data into CockroachDB."""
        self._state.set_phase(MigrationPhase.IMPORTING)

        importer = DataImporter(self._config, self._state, self._crdb, self._schemas)
        importer.import_all(self._tables)

        errors = self._state.get_error_tables()
        if errors:
            logger.warning(
                "%d tables had import errors — continuing with CDC replay for imported tables",
                len(errors),
            )

        self._state.set_phase(MigrationPhase.IMPORT_DONE)

    def _step_cdc_replay(self) -> None:
        """Replay CDC changes until caught up."""
        # Only replay for tables that were successfully imported
        imported_tables = self._state.get_tables_in_status("imported")
        if not imported_tables:
            logger.warning("No tables in 'imported' status — skipping CDC replay")
            self._state.set_phase(MigrationPhase.CDC_REPLAYING)
            return

        replayer = CDCReplayer(
            self._config, self._state, self._sybase, self._crdb, self._schemas
        )
        replayer.replay_loop(imported_tables, until_cutover=True)
        self._state.set_phase(MigrationPhase.CDC_REPLAYING)

    def _step_validate(self) -> None:
        """Validate data consistency between source and target."""
        validator = MigrationValidator(
            self._sybase, self._crdb, self._schemas, self._config
        )

        # Validate imported (and CDC-replayed) tables
        tables_to_validate = (
            self._state.get_tables_in_status("imported")
            + self._state.get_tables_in_status("cdc_replaying")
        )
        if not tables_to_validate:
            tables_to_validate = self._tables

        results = validator.validate_all(tables_to_validate)

        # Update table states based on validation
        for table, result in results.items():
            if result.passed:
                self._state.set_table_status(table, "validated")
            else:
                self._state.set_table_error(table, f"Validation failed: {result.summary()}")

        # Save report
        report = validator.generate_report(results)
        report_path = os.path.join(self._config.log_dir, "validation_report.txt")
        os.makedirs(self._config.log_dir, exist_ok=True)
        with open(report_path, "w") as fh:
            fh.write(report)
        logger.info("Validation report saved to %s", report_path)
        print(report)

        self._state.set_phase(MigrationPhase.VALIDATED)

    def _step_apply_fks(self) -> None:
        """Apply foreign key constraints on CockroachDB."""
        converter = SchemaConverter()
        fk_stmts = converter.generate_foreign_keys(self._schemas)

        if not fk_stmts:
            logger.info("No foreign keys to apply")
            self._state.set_phase(MigrationPhase.FKS_APPLIED)
            return

        logger.info("Applying %d foreign key constraints", len(fk_stmts))
        with self._crdb.connect() as conn:
            cur = conn.cursor()
            for stmt in fk_stmts:
                try:
                    cur.execute(stmt)
                except Exception as exc:
                    logger.error("FK creation failed: %s — %s", stmt, exc)
            conn.commit()
            cur.close()

        self._state.set_phase(MigrationPhase.FKS_APPLIED)

    def _step_cleanup(self) -> None:
        """Remove CDC triggers and log tables from Sybase."""
        cdc = CDCSetup(self._sybase, self._schemas)
        cdc.uninstall_all(self._tables)
        logger.info("CDC artifacts cleaned up from Sybase")
        self._state.set_phase(MigrationPhase.CLEANUP)

    def _step_complete(self) -> None:
        """Mark migration as complete."""
        self._state.set_phase(MigrationPhase.COMPLETE)

        summary = self._state.summary()
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETE")
        logger.info("Total tables: %d", summary["total_tables"])
        logger.info("Status breakdown: %s", summary["status_counts"])
        if summary["error_count"] > 0:
            logger.warning("Tables with errors: %d", summary["error_count"])
            for table, error in self._state.get_error_tables().items():
                logger.warning("  %s: %s", table, error)
        logger.info("=" * 60)

    # ── schema caching ───────────────────────────────────────

    def _save_cached_schemas(self) -> None:
        """Serialize extracted schemas to JSON for resume across restarts."""
        cache_path = os.path.join(self._config.export_dir, "_schemas.json")
        os.makedirs(self._config.export_dir, exist_ok=True)

        data = {}
        for name, schema in self._schemas.items():
            data[name] = {
                "name": schema.name,
                "owner": schema.owner,
                "columns": [
                    {
                        "name": c.name, "datatype": c.datatype,
                        "user_type": c.user_type, "length": c.length,
                        "prec": c.prec, "scale": c.scale,
                        "nullable": c.nullable, "is_identity": c.is_identity,
                        "default_value": c.default_value, "colid": c.colid,
                    }
                    for c in schema.columns
                ],
                "primary_key": schema.primary_key,
                "indexes": [
                    {
                        "name": idx.name, "table_name": idx.table_name,
                        "columns": idx.columns, "is_unique": idx.is_unique,
                        "is_clustered": idx.is_clustered,
                        "is_primary_key": idx.is_primary_key,
                    }
                    for idx in schema.indexes
                ],
                "foreign_keys": [
                    {
                        "constraint_name": fk.constraint_name,
                        "table_name": fk.table_name,
                        "columns": fk.columns, "ref_table": fk.ref_table,
                        "ref_columns": fk.ref_columns,
                    }
                    for fk in schema.foreign_keys
                ],
                "row_count_estimate": schema.row_count_estimate,
            }

        with open(cache_path, "w") as fh:
            json.dump(data, fh, indent=2)
        logger.info("Cached schemas to %s", cache_path)

    def _load_cached_schemas(self) -> None:
        """Load schemas from the JSON cache (for resume)."""
        from .schema_extract import ColumnInfo, ForeignKeyInfo, IndexInfo

        cache_path = os.path.join(self._config.export_dir, "_schemas.json")
        if not os.path.exists(cache_path):
            logger.warning(
                "Schema cache not found at %s — re-extracting from source",
                cache_path,
            )
            self._step_extract_schema()
            return

        with open(cache_path, "r") as fh:
            data = json.load(fh)

        self._schemas = {}
        for name, d in data.items():
            self._schemas[name] = TableSchema(
                name=d["name"],
                owner=d["owner"],
                columns=[
                    ColumnInfo(**c) for c in d["columns"]
                ],
                primary_key=d.get("primary_key"),
                indexes=[
                    IndexInfo(**idx) for idx in d.get("indexes", [])
                ],
                foreign_keys=[
                    ForeignKeyInfo(**fk) for fk in d.get("foreign_keys", [])
                ],
                row_count_estimate=d.get("row_count_estimate", 0),
            )

        self._tables = list(self._schemas.keys())
        logger.info("Loaded cached schemas for %d tables", len(self._schemas))
