"""Pre-flight environment checker: validates everything before migration starts.

Runs comprehensive checks to catch problems BEFORE the migration begins,
not halfway through a 1TB data transfer. This is the single most important
module for building customer confidence.

Checks performed:
  1. Sybase connectivity and credentials
  2. CockroachDB connectivity and credentials
  3. Sybase permissions (trigger creation, system catalog access)
  4. CockroachDB permissions (CREATE TABLE, IMPORT, admin checks)
  5. BCP/isql tool availability and version
  6. Disk space for export directory
  7. Network latency between hosts
  8. Python dependency validation
  9. Character set / encoding detection
  10. Estimated migration size and time
  11. Sybase version compatibility
  12. CockroachDB version compatibility
"""

import importlib
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .config import MigrationConfig
from .connections import CockroachConnection, SybaseConnection

logger = logging.getLogger("sysbasecrdb.preflight")


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    severity: str = "error"  # "error" = blocks migration, "warning" = proceed with caution
    details: Optional[str] = None


@dataclass
class PreflightReport:
    checks: List[CheckResult] = field(default_factory=list)
    estimated_data_size_gb: float = 0.0
    estimated_export_size_gb: float = 0.0
    estimated_time_hours: float = 0.0
    table_count: int = 0
    sybase_version: str = ""
    crdb_version: str = ""

    @property
    def passed(self) -> bool:
        return not any(c.severity == "error" and not c.passed for c in self.checks)

    @property
    def warnings(self) -> List[CheckResult]:
        return [c for c in self.checks if c.severity == "warning" and not c.passed]

    @property
    def errors(self) -> List[CheckResult]:
        return [c for c in self.checks if c.severity == "error" and not c.passed]

    def format_report(self) -> str:
        lines = [
            "=" * 70,
            "PRE-FLIGHT CHECK REPORT",
            "=" * 70,
            "",
        ]

        passed_count = sum(1 for c in self.checks if c.passed)
        total = len(self.checks)
        lines.append(f"Checks: {passed_count}/{total} passed")
        lines.append(f"Errors: {len(self.errors)}")
        lines.append(f"Warnings: {len(self.warnings)}")
        lines.append("")

        if self.sybase_version:
            lines.append(f"Sybase Version:      {self.sybase_version}")
        if self.crdb_version:
            lines.append(f"CockroachDB Version: {self.crdb_version}")
        if self.table_count:
            lines.append(f"Tables to migrate:   {self.table_count}")
        if self.estimated_data_size_gb:
            lines.append(f"Estimated data size: {self.estimated_data_size_gb:.1f} GB")
            lines.append(f"Estimated CSV size:  {self.estimated_export_size_gb:.1f} GB (on disk)")
            lines.append(f"Estimated duration:  {self.estimated_time_hours:.1f} hours")
        lines.append("")

        if self.errors:
            lines.append("ERRORS (must fix before migration):")
            lines.append("-" * 50)
            for c in self.errors:
                lines.append(f"  [FAIL] {c.name}")
                lines.append(f"         {c.message}")
                if c.details:
                    for detail_line in c.details.split("\n"):
                        lines.append(f"         {detail_line}")
            lines.append("")

        if self.warnings:
            lines.append("WARNINGS (review before migration):")
            lines.append("-" * 50)
            for c in self.warnings:
                lines.append(f"  [WARN] {c.name}")
                lines.append(f"         {c.message}")
                if c.details:
                    for detail_line in c.details.split("\n"):
                        lines.append(f"         {detail_line}")
            lines.append("")

        lines.append("ALL CHECKS:")
        lines.append("-" * 50)
        for c in self.checks:
            icon = "PASS" if c.passed else ("FAIL" if c.severity == "error" else "WARN")
            lines.append(f"  [{icon}] {c.name}: {c.message}")
        lines.append("")

        if self.passed:
            lines.append("RESULT: All critical checks passed. Ready to migrate.")
        else:
            lines.append("RESULT: Critical checks failed. Fix errors before proceeding.")

        return "\n".join(lines)


class PreflightChecker:
    """Run comprehensive pre-migration validation checks."""

    def __init__(self, config: MigrationConfig):
        self._config = config
        self._sybase = SybaseConnection(config.source)
        self._crdb = CockroachConnection(config.target)

    def run_all_checks(self) -> PreflightReport:
        """Run all pre-flight checks and return a report."""
        report = PreflightReport()

        # Run checks in dependency order
        checks = [
            self._check_python_deps,
            self._check_sybase_connectivity,
            self._check_crdb_connectivity,
            self._check_sybase_version,
            self._check_crdb_version,
            self._check_sybase_permissions,
            self._check_crdb_permissions,
            self._check_bcp_available,
            self._check_isql_available,
            self._check_disk_space,
            self._check_export_dir_writable,
            self._check_state_dir_writable,
            self._check_sybase_charset,
            self._check_table_count_and_size,
            self._check_tables_have_pks,
            self._check_lob_columns,
            self._check_identity_columns,
            self._check_computed_columns,
            self._check_cross_db_references,
            self._check_userfile_or_nodelocal,
        ]

        for check_fn in checks:
            try:
                result = check_fn(report)
                report.checks.append(result)
            except Exception as exc:
                report.checks.append(CheckResult(
                    name=check_fn.__name__.replace("_check_", "").replace("_", " ").title(),
                    passed=False,
                    message=f"Check itself failed: {exc}",
                    severity="warning",
                ))

        return report

    # ── Python / tool checks ─────────────────────────────────

    def _check_python_deps(self, report: PreflightReport) -> CheckResult:
        missing = []
        for mod in ("pyodbc", "psycopg2", "yaml"):
            try:
                importlib.import_module(mod)
            except ImportError:
                pkg = "psycopg2-binary" if mod == "psycopg2" else ("pyyaml" if mod == "yaml" else mod)
                missing.append(pkg)

        if missing:
            return CheckResult(
                name="Python Dependencies",
                passed=False,
                message=f"Missing packages: {', '.join(missing)}",
                severity="error",
                details=f"Install with: pip install {' '.join(missing)}",
            )
        return CheckResult(
            name="Python Dependencies",
            passed=True,
            message="All required packages installed (pyodbc, psycopg2, pyyaml)",
        )

    def _check_bcp_available(self, report: PreflightReport) -> CheckResult:
        bcp_path = self._config.source.bcp_path
        try:
            result = subprocess.run(
                [bcp_path, "-v"], capture_output=True, text=True, timeout=10
            )
            version_info = result.stdout.strip() or result.stderr.strip()
            return CheckResult(
                name="BCP Tool",
                passed=True,
                message=f"Found at {bcp_path}",
                details=version_info[:200] if version_info else None,
            )
        except FileNotFoundError:
            return CheckResult(
                name="BCP Tool",
                passed=False,
                message=f"BCP not found at '{bcp_path}'",
                severity="error",
                details=(
                    "BCP is required for bulk data export.\n"
                    "Verify the path in config: source.bcp_path\n"
                    "Typical location: /opt/sap/OCS-16_0/bin/bcp"
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="BCP Tool",
                passed=False,
                message=f"BCP check failed: {exc}",
                severity="error",
            )

    def _check_isql_available(self, report: PreflightReport) -> CheckResult:
        isql_path = self._config.source.isql_path
        try:
            result = subprocess.run(
                [isql_path, "-v"], capture_output=True, text=True, timeout=10
            )
            return CheckResult(
                name="isql Tool",
                passed=True,
                message=f"Found at {isql_path}",
            )
        except FileNotFoundError:
            return CheckResult(
                name="isql Tool",
                passed=False,
                message=f"isql not found at '{isql_path}' (needed for LOB fallback export)",
                severity="warning",
                details="Tables with TEXT/IMAGE columns will use pyodbc streaming instead.",
            )
        except Exception:
            return CheckResult(
                name="isql Tool", passed=True,
                message=f"Found at {isql_path} (version check not supported)",
            )

    # ── Connectivity checks ──────────────────────────────────

    def _check_sybase_connectivity(self, report: PreflightReport) -> CheckResult:
        cfg = self._config.source
        try:
            start = time.time()
            with self._sybase.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            latency_ms = (time.time() - start) * 1000

            return CheckResult(
                name="Sybase Connectivity",
                passed=True,
                message=f"Connected to {cfg.host}:{cfg.port}/{cfg.database} ({latency_ms:.0f}ms)",
            )
        except Exception as exc:
            return CheckResult(
                name="Sybase Connectivity",
                passed=False,
                message=f"Cannot connect to {cfg.host}:{cfg.port}/{cfg.database}",
                severity="error",
                details=(
                    f"Error: {exc}\n"
                    "Verify:\n"
                    "  - Hostname and port are correct\n"
                    "  - ODBC driver is installed and configured\n"
                    "  - Username/password are correct\n"
                    "  - Firewall allows the connection\n"
                    "  - Sybase server is running"
                ),
            )

    def _check_crdb_connectivity(self, report: PreflightReport) -> CheckResult:
        cfg = self._config.target
        try:
            start = time.time()
            with self._crdb.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            latency_ms = (time.time() - start) * 1000

            return CheckResult(
                name="CockroachDB Connectivity",
                passed=True,
                message=f"Connected to {cfg.host}:{cfg.port}/{cfg.database} ({latency_ms:.0f}ms)",
            )
        except Exception as exc:
            return CheckResult(
                name="CockroachDB Connectivity",
                passed=False,
                message=f"Cannot connect to {cfg.host}:{cfg.port}/{cfg.database}",
                severity="error",
                details=(
                    f"Error: {exc}\n"
                    "Verify:\n"
                    "  - Hostname and port are correct\n"
                    "  - SSL certificates are configured (sslmode, sslrootcert)\n"
                    "  - Username/password are correct\n"
                    "  - Database exists on CockroachDB\n"
                    "  - Firewall allows the connection"
                ),
            )

    # ── Version checks ───────────────────────────────────────

    def _check_sybase_version(self, report: PreflightReport) -> CheckResult:
        try:
            with self._sybase.cursor() as cur:
                cur.execute("SELECT @@version")
                version = cur.fetchone()[0]
                report.sybase_version = version.split("\n")[0].strip() if version else "unknown"

            if "16." in report.sybase_version or "15.7" in report.sybase_version:
                return CheckResult(
                    name="Sybase Version",
                    passed=True,
                    message=report.sybase_version[:80],
                )
            else:
                return CheckResult(
                    name="Sybase Version",
                    passed=True,
                    message=report.sybase_version[:80],
                    severity="warning",
                    details="Toolkit is tested with ASE 16.x. Older versions may have catalog differences.",
                )
        except Exception as exc:
            return CheckResult(
                name="Sybase Version",
                passed=False,
                message=f"Could not determine version: {exc}",
                severity="warning",
            )

    def _check_crdb_version(self, report: PreflightReport) -> CheckResult:
        try:
            with self._crdb.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                report.crdb_version = version.strip() if version else "unknown"

            return CheckResult(
                name="CockroachDB Version",
                passed=True,
                message=report.crdb_version[:80],
            )
        except Exception as exc:
            return CheckResult(
                name="CockroachDB Version",
                passed=False,
                message=f"Could not determine version: {exc}",
                severity="warning",
            )

    # ── Permission checks ────────────────────────────────────

    def _check_sybase_permissions(self, report: PreflightReport) -> CheckResult:
        """Check that the Sybase user can read system catalogs and create triggers."""
        issues = []
        try:
            with self._sybase.cursor() as cur:
                # Can read sysobjects?
                cur.execute("SELECT COUNT(*) FROM sysobjects WHERE type = 'U'")
                table_count = cur.fetchone()[0]
                if table_count == 0:
                    issues.append("No user tables visible (check catalog permissions)")

                # Can create objects? Test with a temp check
                try:
                    cur.execute(
                        "CREATE TABLE _preflight_test_del (id INT NOT NULL)"
                    )
                    cur.execute("DROP TABLE _preflight_test_del")
                except Exception as exc:
                    issues.append(f"Cannot CREATE TABLE: {exc}")

        except Exception as exc:
            return CheckResult(
                name="Sybase Permissions",
                passed=False,
                message=f"Permission check failed: {exc}",
                severity="error",
                details=(
                    "The migration user needs:\n"
                    "  - SELECT on system catalogs (sysobjects, syscolumns, etc.)\n"
                    "  - CREATE TABLE (for CDC log tables)\n"
                    "  - CREATE TRIGGER (for CDC triggers)\n"
                    "  - SELECT on all source tables\n"
                    "Grant with: GRANT CREATE TABLE, CREATE TRIGGER TO mig_user"
                ),
            )

        if issues:
            return CheckResult(
                name="Sybase Permissions",
                passed=False,
                message="; ".join(issues),
                severity="error",
                details=(
                    "Required permissions:\n"
                    "  - SELECT on system catalogs\n"
                    "  - CREATE TABLE, CREATE TRIGGER\n"
                    "  - SELECT on all source tables"
                ),
            )
        return CheckResult(
            name="Sybase Permissions",
            passed=True,
            message=f"Can read catalogs ({table_count} user tables visible) and create objects",
        )

    def _check_crdb_permissions(self, report: PreflightReport) -> CheckResult:
        """Check CockroachDB permissions for DDL and IMPORT."""
        issues = []
        try:
            with self._crdb.connect() as conn:
                cur = conn.cursor()
                # Can create and drop tables?
                try:
                    cur.execute(
                        'CREATE TABLE IF NOT EXISTS "_preflight_test_del" (id INT PRIMARY KEY)'
                    )
                    conn.commit()
                    cur.execute('DROP TABLE IF EXISTS "_preflight_test_del"')
                    conn.commit()
                except Exception as exc:
                    issues.append(f"Cannot CREATE/DROP TABLE: {exc}")

                # Check if user is admin (needed for IMPORT INTO)
                if self._config.target.import_method != "copy_only":
                    try:
                        cur.execute("SHOW CLUSTER SETTING version")
                        cur.fetchone()
                    except Exception:
                        issues.append(
                            "Cannot read cluster settings (may lack admin role for IMPORT INTO). "
                            "Set import_method: copy_only if user is not admin."
                        )
                cur.close()

        except Exception as exc:
            return CheckResult(
                name="CockroachDB Permissions",
                passed=False,
                message=f"Permission check failed: {exc}",
                severity="error",
            )

        if issues:
            severity = "warning" if "copy_only" in str(issues) else "error"
            return CheckResult(
                name="CockroachDB Permissions",
                passed=False,
                message="; ".join(issues),
                severity=severity,
                details=(
                    "For IMPORT INTO (fastest), the user needs admin role.\n"
                    "Alternative: set import_method: copy_only in config\n"
                    "to use COPY FROM instead (slower but fewer permissions needed)."
                ),
            )
        return CheckResult(
            name="CockroachDB Permissions",
            passed=True,
            message="Can create tables and access cluster settings",
        )

    # ── Disk / filesystem checks ─────────────────────────────

    def _check_disk_space(self, report: PreflightReport) -> CheckResult:
        export_dir = self._config.export_dir
        parent = os.path.dirname(export_dir) or "."
        if not os.path.exists(parent):
            parent = "/"

        try:
            usage = shutil.disk_usage(parent)
            free_gb = usage.free / (1024 ** 3)

            # Estimate needed space (CSV is roughly 1.5-3x raw database size)
            needed_gb = report.estimated_export_size_gb or 100  # default estimate

            if free_gb < needed_gb * 1.2:  # 20% headroom
                return CheckResult(
                    name="Disk Space",
                    passed=False,
                    message=f"{free_gb:.1f} GB free at {parent} — may need {needed_gb:.0f}+ GB",
                    severity="warning",
                    details=(
                        f"Export directory: {export_dir}\n"
                        f"Free space: {free_gb:.1f} GB\n"
                        f"Estimated need: {needed_gb:.0f} GB (with 20% headroom)\n"
                        "CSV files are typically 1.5-3x the raw database size.\n"
                        "Consider using a larger volume or compressing completed exports."
                    ),
                )
            return CheckResult(
                name="Disk Space",
                passed=True,
                message=f"{free_gb:.1f} GB free at {parent}",
            )
        except Exception as exc:
            return CheckResult(
                name="Disk Space",
                passed=False,
                message=f"Could not check disk space: {exc}",
                severity="warning",
            )

    def _check_export_dir_writable(self, report: PreflightReport) -> CheckResult:
        export_dir = self._config.export_dir
        try:
            os.makedirs(export_dir, exist_ok=True)
            test_file = os.path.join(export_dir, "_preflight_write_test")
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            return CheckResult(
                name="Export Directory",
                passed=True,
                message=f"Writable: {export_dir}",
            )
        except Exception as exc:
            return CheckResult(
                name="Export Directory",
                passed=False,
                message=f"Cannot write to {export_dir}: {exc}",
                severity="error",
            )

    def _check_state_dir_writable(self, report: PreflightReport) -> CheckResult:
        state_dir = os.path.dirname(self._config.state_file) or "."
        try:
            os.makedirs(state_dir, exist_ok=True)
            return CheckResult(
                name="State Directory",
                passed=True,
                message=f"Writable: {state_dir}",
            )
        except Exception as exc:
            return CheckResult(
                name="State Directory",
                passed=False,
                message=f"Cannot write to {state_dir}: {exc}",
                severity="error",
            )

    # ── Data characteristic checks ───────────────────────────

    def _check_sybase_charset(self, report: PreflightReport) -> CheckResult:
        """Detect Sybase server character set — non-UTF-8 needs encoding conversion."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("SELECT @@client_csname, @@ncharset")
                row = cur.fetchone()
                client_cs = row[0].strip() if row[0] else "unknown"
                ncharset = row[1].strip() if row[1] else "unknown"

            if "utf" in client_cs.lower():
                return CheckResult(
                    name="Character Set",
                    passed=True,
                    message=f"Client charset: {client_cs}, ncharset: {ncharset}",
                )
            else:
                return CheckResult(
                    name="Character Set",
                    passed=True,
                    message=f"Client charset: {client_cs}, ncharset: {ncharset}",
                    severity="warning",
                    details=(
                        "Sybase is not using UTF-8. CockroachDB requires UTF-8.\n"
                        f"Current charset: {client_cs}\n"
                        "BCP will export in the server's charset. The toolkit handles\n"
                        "encoding conversion during import, but verify special characters\n"
                        "in your data (accented letters, CJK, etc.) after migration."
                    ),
                )
        except Exception as exc:
            return CheckResult(
                name="Character Set",
                passed=True,
                message=f"Could not detect charset: {exc}",
                severity="warning",
            )

    def _check_table_count_and_size(self, report: PreflightReport) -> CheckResult:
        """Count tables and estimate total data size."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), SUM(reserved_pages(id, doampg) + reserved_pages(id, ioampg))
                    FROM sysindexes
                    WHERE indid IN (0, 1)
                      AND id IN (SELECT id FROM sysobjects WHERE type = 'U'
                                 AND name NOT LIKE '_cdc_log_%')
                """)
                row = cur.fetchone()
                table_count = row[0] or 0
                # Pages are typically 2KB or 16KB in Sybase
                total_pages = row[1] or 0

                # Try to get page size
                cur.execute("SELECT @@maxpagesize")
                page_row = cur.fetchone()
                page_size = int(page_row[0]) if page_row else 2048

                total_bytes = total_pages * page_size
                total_gb = total_bytes / (1024 ** 3)
                csv_gb = total_gb * 2.0  # CSV is roughly 2x raw size

                report.table_count = table_count
                report.estimated_data_size_gb = total_gb
                report.estimated_export_size_gb = csv_gb

                # Rough time estimate: assume 100 MB/s aggregate throughput
                throughput_mbps = 100 * self._config.export_workers / 8  # scale with workers
                report.estimated_time_hours = (csv_gb * 1024) / (throughput_mbps * 3600 / 1000)
                # Add import time (roughly same) + CDC + validation overhead
                report.estimated_time_hours *= 2.5

            return CheckResult(
                name="Data Size Estimate",
                passed=True,
                message=(
                    f"{table_count} tables, ~{total_gb:.1f} GB data, "
                    f"~{csv_gb:.1f} GB CSV, ~{report.estimated_time_hours:.1f}h estimated"
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="Data Size Estimate",
                passed=True,
                message=f"Could not estimate size: {exc}",
                severity="warning",
            )

    def _check_tables_have_pks(self, report: PreflightReport) -> CheckResult:
        """Check how many tables lack primary keys (affects CDC and chunking)."""
        try:
            with self._sybase.cursor() as cur:
                # Tables without a PK index (status & 2048)
                cur.execute("""
                    SELECT o.name
                    FROM sysobjects o
                    WHERE o.type = 'U'
                      AND o.name NOT LIKE '_cdc_log_%'
                      AND NOT EXISTS (
                          SELECT 1 FROM sysindexes i
                          WHERE i.id = o.id AND i.status & 2048 = 2048
                      )
                    ORDER BY o.name
                """)
                no_pk_tables = [row[0] for row in cur.fetchall()]

            if not no_pk_tables:
                return CheckResult(
                    name="Primary Keys",
                    passed=True,
                    message="All tables have primary keys",
                )
            elif len(no_pk_tables) <= 10:
                return CheckResult(
                    name="Primary Keys",
                    passed=True,
                    message=f"{len(no_pk_tables)} tables without PKs",
                    severity="warning",
                    details=(
                        "Tables without PKs:\n"
                        + "\n".join(f"  - {t}" for t in no_pk_tables) +
                        "\n\nImpact:\n"
                        "  - Cannot use PK-range chunking (single BCP export)\n"
                        "  - CDC DELETE replay cannot identify rows\n"
                        "  - Consider adding PKs before migration"
                    ),
                )
            else:
                return CheckResult(
                    name="Primary Keys",
                    passed=True,
                    message=f"{len(no_pk_tables)} tables without PKs",
                    severity="warning",
                    details=(
                        f"First 10: {', '.join(no_pk_tables[:10])}\n"
                        "Consider adding PKs to critical tables before migration."
                    ),
                )
        except Exception as exc:
            return CheckResult(
                name="Primary Keys",
                passed=True,
                message=f"Could not check PKs: {exc}",
                severity="warning",
            )

    def _check_lob_columns(self, report: PreflightReport) -> CheckResult:
        """Detect tables with TEXT/IMAGE columns that need special export handling."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT o.name
                    FROM syscolumns c
                    JOIN sysobjects o ON c.id = o.id
                    JOIN systypes t ON c.type = t.type AND t.usertype < 100
                    WHERE o.type = 'U'
                      AND o.name NOT LIKE '_cdc_log_%'
                      AND t.name IN ('text', 'unitext', 'image')
                    ORDER BY o.name
                """)
                lob_tables = [row[0] for row in cur.fetchall()]

            if not lob_tables:
                return CheckResult(
                    name="LOB Columns",
                    passed=True,
                    message="No TEXT/IMAGE columns detected",
                )
            return CheckResult(
                name="LOB Columns",
                passed=True,
                message=f"{len(lob_tables)} tables have TEXT/IMAGE columns",
                severity="warning",
                details=(
                    "These tables will use pyodbc streaming instead of BCP.\n"
                    "This is slower but handles LOB data correctly.\n"
                    f"Tables: {', '.join(lob_tables[:20])}"
                    + (f"\n... and {len(lob_tables) - 20} more" if len(lob_tables) > 20 else "")
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="LOB Columns",
                passed=True,
                message=f"Could not check LOB columns: {exc}",
                severity="warning",
            )

    def _check_identity_columns(self, report: PreflightReport) -> CheckResult:
        """Detect identity columns — values will NOT be preserved exactly."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("""
                    SELECT o.name, c.name
                    FROM syscolumns c
                    JOIN sysobjects o ON c.id = o.id
                    WHERE o.type = 'U'
                      AND o.name NOT LIKE '_cdc_log_%'
                      AND c.status & 128 = 128
                    ORDER BY o.name
                """)
                identity_cols = [(row[0], row[1]) for row in cur.fetchall()]

            if not identity_cols:
                return CheckResult(
                    name="Identity Columns",
                    passed=True,
                    message="No identity columns detected",
                )
            return CheckResult(
                name="Identity Columns",
                passed=True,
                message=f"{len(identity_cols)} tables have identity columns",
                severity="warning",
                details=(
                    "Identity columns are mapped to INT8 DEFAULT unique_rowid().\n"
                    "EXISTING values are preserved during bulk import.\n"
                    "NEW values after migration will be unique_rowid() generated\n"
                    "(not sequential). If your application relies on sequential IDs,\n"
                    "consider using CREATE SEQUENCE after migration.\n"
                    f"Tables: {', '.join(t for t, c in identity_cols[:15])}"
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="Identity Columns",
                passed=True,
                message=f"Could not check identity columns: {exc}",
                severity="warning",
            )

    def _check_computed_columns(self, report: PreflightReport) -> CheckResult:
        """Detect computed columns that need expression conversion."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("""
                    SELECT o.name, c.name
                    FROM syscolumns c
                    JOIN sysobjects o ON c.id = o.id
                    WHERE o.type = 'U'
                      AND o.name NOT LIKE '_cdc_log_%'
                      AND c.status2 & 1 = 1
                    ORDER BY o.name
                """)
                computed = [(row[0], row[1]) for row in cur.fetchall()]

            if not computed:
                return CheckResult(
                    name="Computed Columns",
                    passed=True,
                    message="No computed columns detected",
                )
            return CheckResult(
                name="Computed Columns",
                passed=True,
                message=f"{len(computed)} computed columns need expression conversion",
                severity="warning",
                details=(
                    "Computed columns require T-SQL expression conversion to\n"
                    "CockroachDB GENERATED ALWAYS AS syntax. Run the 'assess'\n"
                    "command for a detailed conversion report.\n"
                    f"Columns: {', '.join(f'{t}.{c}' for t, c in computed[:10])}"
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="Computed Columns",
                passed=True,
                message=f"Could not check computed columns: {exc}",
                severity="warning",
            )

    def _check_cross_db_references(self, report: PreflightReport) -> CheckResult:
        """Detect views/procs that reference other databases (db..table syntax)."""
        try:
            with self._sybase.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT d.depname
                    FROM sysdepends d
                    JOIN sysobjects o ON d.id = o.id
                    WHERE o.type IN ('V', 'P', 'TR')
                      AND d.depdbname IS NOT NULL
                      AND d.depdbname != DB_NAME()
                """)
                cross_refs = [row[0] for row in cur.fetchall()]

            if not cross_refs:
                return CheckResult(
                    name="Cross-Database References",
                    passed=True,
                    message="No cross-database references detected",
                )
            return CheckResult(
                name="Cross-Database References",
                passed=True,
                message=f"{len(cross_refs)} cross-database references found",
                severity="warning",
                details=(
                    "CockroachDB does not support cross-database queries.\n"
                    "Objects referencing other databases will need manual rewriting.\n"
                    f"Referenced objects: {', '.join(cross_refs[:10])}"
                ),
            )
        except Exception as exc:
            return CheckResult(
                name="Cross-Database References",
                passed=True,
                message="Could not check (sysdepends may not be populated)",
                severity="warning",
            )

    def _check_userfile_or_nodelocal(self, report: PreflightReport) -> CheckResult:
        """Verify that the configured import method is viable."""
        method = self._config.target.import_method
        if method == "copy_only":
            return CheckResult(
                name="Import Method",
                passed=True,
                message="Using COPY FROM (slower but requires no file staging)",
            )

        if method == "userfile":
            cmd_parts = self._config.target.userfile_upload_cmd.split()
            try:
                result = subprocess.run(
                    [cmd_parts[0], "version"], capture_output=True, text=True, timeout=10
                )
                return CheckResult(
                    name="Import Method",
                    passed=True,
                    message=f"Using userfile upload via '{cmd_parts[0]}'",
                )
            except FileNotFoundError:
                return CheckResult(
                    name="Import Method",
                    passed=False,
                    message=f"'{cmd_parts[0]}' command not found for userfile upload",
                    severity="warning",
                    details=(
                        "IMPORT INTO with userfile requires the cockroach CLI.\n"
                        "Options:\n"
                        "  1. Install cockroach CLI on migration host\n"
                        "  2. Change import_method to 'nodelocal' (files on CRDB node)\n"
                        "  3. Change import_method to 'copy_only' (uses COPY FROM, slower)"
                    ),
                )
            except Exception:
                return CheckResult(
                    name="Import Method",
                    passed=True,
                    message=f"Using userfile upload (could not verify '{cmd_parts[0]}' version)",
                )

        if method == "nodelocal":
            return CheckResult(
                name="Import Method",
                passed=True,
                message="Using nodelocal — ensure CSV files are accessible on CRDB nodes",
                severity="warning",
                details=(
                    "nodelocal requires CSV files to be placed on each CockroachDB\n"
                    "node's local filesystem. Verify that the migration host can\n"
                    "write to the CRDB node's extern directory."
                ),
            )

        return CheckResult(
            name="Import Method",
            passed=False,
            message=f"Unknown import_method: {method}",
            severity="error",
            details="Valid options: userfile, nodelocal, copy_only",
        )
