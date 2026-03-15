"""Compatibility assessment report generator.

Analyzes the full Sybase database schema and produces a detailed report
showing exactly what converts automatically, what needs manual work,
and what is incompatible. This is the key tool for building customer
confidence BEFORE committing to migration.

Usage:
    python migrate.py -c config/migration.yaml assess

Output:
    assessment_report.txt — detailed per-object conversion analysis
    assessment_summary.json — machine-readable summary for dashboards
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .config import MigrationConfig
from .connections import SybaseConnection
from .schema_convert import SchemaConverter, TSQLRewriter, SYBASE_TO_CRDB_TYPE_MAP
from .schema_extract import (
    DatabaseSchema,
    SchemaExtractor,
    StoredProcInfo,
    UserFunctionInfo,
    ViewInfo,
)

logger = logging.getLogger("sysbasecrdb.assessment")


# Sybase constructs that have NO CockroachDB equivalent
_UNSUPPORTED_CONSTRUCTS = [
    (r'\bxp_\w+', "Extended stored procedure (xp_*) — no equivalent, must rewrite as external script"),
    (r'\bGOTO\s+\w+', "GOTO statement — no equivalent in PL/pgSQL, must restructure control flow"),
    (r'\bWAITFOR\b', "WAITFOR statement — use pg_sleep() instead"),
    (r'\bREADTEXT\b', "READTEXT — no equivalent, use standard SELECT on TEXT columns"),
    (r'\bWRITETEXT\b', "WRITETEXT — no equivalent, use standard UPDATE"),
    (r'\bBULK\s+INSERT\b', "BULK INSERT — use COPY or IMPORT INTO instead"),
    (r'\bOPENROWSET\b', "OPENROWSET — no equivalent, use foreign data wrappers"),
    (r'\bCURSOR\b.*\bFOR\s+READ\s+ONLY\b', "READ ONLY cursor — use standard cursor, read-only is default"),
    (r'\bSET\s+ROWCOUNT\b', "SET ROWCOUNT — use LIMIT instead"),
    (r'\bSET\s+ANSI_NULLS\b', "SET ANSI_NULLS — CockroachDB always uses ANSI NULL behavior"),
    (r'\bSET\s+QUOTED_IDENTIFIER\b', "SET QUOTED_IDENTIFIER — not applicable"),
    (r'\bSET\s+NOCOUNT\b', "SET NOCOUNT — not applicable (CockroachDB doesn't send count by default)"),
    (r'\bRAISERROR\b', "RAISERROR — use RAISE EXCEPTION in PL/pgSQL"),
    (r'\bEXEC(UTE)?\s*\(', "Dynamic SQL EXEC — use EXECUTE in PL/pgSQL"),
    (r'\bsp_executesql\b', "sp_executesql — use EXECUTE with format() in PL/pgSQL"),
    (r'\bDBCC\b', "DBCC command — no equivalent (database-specific maintenance)"),
    (r'\bTRUNCATE\s+TABLE\b', "TRUNCATE TABLE — supported in CockroachDB (same syntax)"),
    (r'##\w+', "Global temp table (##name) — CockroachDB has no global temp tables"),
    (r'#\w+', "Local temp table (#name) — use CREATE TEMP TABLE instead"),
    (r'\bINTO\s+#', "SELECT INTO #temp — use CREATE TEMP TABLE + INSERT"),
]

# Constructs that convert with caveats
_PARTIAL_CONSTRUCTS = [
    (r'\bIDENTITY\b', "IDENTITY column — maps to unique_rowid() (non-sequential)"),
    (r'\bTOP\s+\d+\s+PERCENT\b', "TOP PERCENT — no direct equivalent, needs subquery rewrite"),
    (r'\bCOMPUTE\b', "COMPUTE clause — no equivalent, use GROUP BY with ROLLUP/CUBE"),
    (r'\bTEXTVALID\b', "TEXTVALID — no equivalent, text pointers don't exist in CockroachDB"),
    (r'\bTEXTPTR\b', "TEXTPTR — no equivalent"),
    (r'\btimestamp\b', "timestamp datatype — maps to BYTES (it's a rowversion, not a datetime)"),
]


@dataclass
class ObjectAssessment:
    name: str
    object_type: str  # table, view, proc, function, trigger, udt, rule
    conversion_status: str  # "auto", "partial", "manual", "unsupported"
    auto_converted: List[str] = field(default_factory=list)
    needs_review: List[str] = field(default_factory=list)
    unsupported: List[str] = field(default_factory=list)
    converted_ddl: Optional[str] = None


@dataclass
class AssessmentReport:
    database: str
    total_objects: int = 0
    auto_count: int = 0
    partial_count: int = 0
    manual_count: int = 0
    unsupported_count: int = 0
    objects: List[ObjectAssessment] = field(default_factory=list)
    type_coverage: Dict[str, int] = field(default_factory=dict)
    unmapped_types: List[str] = field(default_factory=list)
    summary: Dict[str, dict] = field(default_factory=dict)


class CompatibilityAssessor:
    """Analyze a Sybase database and produce a conversion compatibility report."""

    def __init__(self, config: MigrationConfig):
        self._config = config
        self._sybase = SybaseConnection(config.source)
        self._converter = SchemaConverter()
        self._rewriter = TSQLRewriter()

    def run_assessment(self) -> AssessmentReport:
        """Run full compatibility assessment against the Sybase database."""
        logger.info("Starting compatibility assessment...")

        extractor = SchemaExtractor(self._sybase, self._config.source.database)
        db_schema = extractor.get_full_database_schema(
            self._config.table_include, self._config.table_exclude,
        )

        report = AssessmentReport(database=self._config.source.database)

        # Assess each object type
        self._assess_tables(db_schema, report)
        self._assess_views(db_schema, report)
        self._assess_stored_procs(db_schema, report)
        self._assess_functions(db_schema, report)
        self._assess_triggers(db_schema, report)
        self._assess_udts(db_schema, report)
        self._assess_rules(db_schema, report)

        # Compute totals
        report.total_objects = len(report.objects)
        report.auto_count = sum(1 for o in report.objects if o.conversion_status == "auto")
        report.partial_count = sum(1 for o in report.objects if o.conversion_status == "partial")
        report.manual_count = sum(1 for o in report.objects if o.conversion_status == "manual")
        report.unsupported_count = sum(1 for o in report.objects if o.conversion_status == "unsupported")

        # Type coverage
        for o in report.objects:
            report.type_coverage[o.object_type] = report.type_coverage.get(o.object_type, 0) + 1

        # Summary by type
        for obj_type in set(o.object_type for o in report.objects):
            type_objs = [o for o in report.objects if o.object_type == obj_type]
            report.summary[obj_type] = {
                "total": len(type_objs),
                "auto": sum(1 for o in type_objs if o.conversion_status == "auto"),
                "partial": sum(1 for o in type_objs if o.conversion_status == "partial"),
                "manual": sum(1 for o in type_objs if o.conversion_status == "manual"),
                "unsupported": sum(1 for o in type_objs if o.conversion_status == "unsupported"),
            }

        logger.info(
            "Assessment complete: %d objects (auto=%d, partial=%d, manual=%d, unsupported=%d)",
            report.total_objects, report.auto_count, report.partial_count,
            report.manual_count, report.unsupported_count,
        )
        return report

    # ── Per-type assessment ──────────────────────────────────

    def _assess_tables(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, schema in db_schema.tables.items():
            assessment = ObjectAssessment(name=name, object_type="table")
            issues = []

            # Check column types
            for col in schema.columns:
                base = col.datatype.lower().strip()
                if base in SYBASE_TO_CRDB_TYPE_MAP:
                    assessment.auto_converted.append(f"Column {col.name}: {col.datatype} → mapped")
                else:
                    issues.append(f"Column {col.name}: unmapped type '{col.datatype}' (will default to TEXT)")
                    if col.datatype not in report.unmapped_types:
                        report.unmapped_types.append(col.datatype)

                if col.is_computed:
                    if col.computed_formula:
                        rewritten = self._rewriter.rewrite(col.computed_formula)
                        if self._rewriter.warnings:
                            issues.extend(
                                f"Computed column {col.name}: {w}" for w in self._rewriter.warnings
                            )
                        else:
                            assessment.auto_converted.append(
                                f"Computed column {col.name}: auto-converted to GENERATED ALWAYS AS"
                            )
                    else:
                        issues.append(f"Computed column {col.name}: formula could not be extracted")

                if col.default_value:
                    converted = self._converter.convert_default(col.default_value)
                    if converted:
                        assessment.auto_converted.append(f"Default {col.name}: '{col.default_value}' → '{converted}'")
                    else:
                        issues.append(f"Default {col.name}: could not convert '{col.default_value}'")

            # Check constraints
            for chk in schema.check_constraints:
                scan = self._scan_for_constructs(chk.definition)
                if scan["unsupported"]:
                    issues.extend(f"CHECK {chk.constraint_name}: {u}" for u in scan["unsupported"])
                else:
                    assessment.auto_converted.append(f"CHECK {chk.constraint_name}: auto-converted")

            # Determine status
            if issues:
                assessment.needs_review = issues
                assessment.conversion_status = "partial"
            else:
                assessment.conversion_status = "auto"

            try:
                assessment.converted_ddl = self._converter.generate_create_table(schema)
            except Exception as exc:
                assessment.unsupported.append(f"DDL generation failed: {exc}")
                assessment.conversion_status = "manual"

            report.objects.append(assessment)

    def _assess_views(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, view in db_schema.views.items():
            assessment = ObjectAssessment(name=name, object_type="view")
            scan = self._scan_for_constructs(view.source_sql)

            if scan["unsupported"]:
                assessment.unsupported = scan["unsupported"]
                assessment.conversion_status = "manual"
            elif scan["partial"]:
                assessment.needs_review = scan["partial"]
                assessment.conversion_status = "partial"
            else:
                assessment.conversion_status = "auto"
                assessment.auto_converted.append("View SQL auto-converted")

            try:
                ddl, warnings = self._converter.convert_view(view)
                assessment.converted_ddl = ddl
                assessment.needs_review.extend(warnings)
                if warnings and assessment.conversion_status == "auto":
                    assessment.conversion_status = "partial"
            except Exception as exc:
                assessment.unsupported.append(f"Conversion failed: {exc}")
                assessment.conversion_status = "manual"

            report.objects.append(assessment)

    def _assess_stored_procs(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, proc in db_schema.stored_procs.items():
            assessment = ObjectAssessment(name=name, object_type="stored_procedure")
            scan = self._scan_for_constructs(proc.source_sql)

            # Stored procs always need at least partial review
            assessment.needs_review.append(
                "T-SQL stored procedure → PL/pgSQL conversion requires manual review"
            )

            if scan["unsupported"]:
                assessment.unsupported = scan["unsupported"]
                assessment.conversion_status = "manual"
            elif scan["partial"]:
                assessment.needs_review.extend(scan["partial"])
                assessment.conversion_status = "manual"
            else:
                assessment.conversion_status = "partial"

            try:
                ddl, warnings = self._converter.convert_stored_proc(proc)
                assessment.converted_ddl = ddl
                assessment.needs_review.extend(warnings)
            except Exception as exc:
                assessment.unsupported.append(f"Scaffold generation failed: {exc}")

            report.objects.append(assessment)

    def _assess_functions(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, func in db_schema.functions.items():
            assessment = ObjectAssessment(name=name, object_type="function")
            scan = self._scan_for_constructs(func.source_sql)

            assessment.needs_review.append(
                "User function → PL/pgSQL conversion requires manual review"
            )

            if scan["unsupported"]:
                assessment.unsupported = scan["unsupported"]
                assessment.conversion_status = "manual"
            else:
                assessment.conversion_status = "partial"

            report.objects.append(assessment)

    def _assess_triggers(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, trigger in db_schema.triggers.items():
            assessment = ObjectAssessment(name=name, object_type="trigger")
            scan = self._scan_for_constructs(trigger.source_sql)

            assessment.needs_review.append(
                f"Trigger on {trigger.table_name} ({', '.join(trigger.events)}) — "
                "CockroachDB trigger syntax differs from Sybase"
            )

            if scan["unsupported"]:
                assessment.unsupported = scan["unsupported"]
                assessment.conversion_status = "manual"
            else:
                assessment.conversion_status = "partial"

            report.objects.append(assessment)

    def _assess_udts(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, udt in db_schema.user_types.items():
            assessment = ObjectAssessment(name=name, object_type="user_type")

            base = udt.base_type.lower()
            if base in SYBASE_TO_CRDB_TYPE_MAP:
                assessment.auto_converted.append(
                    f"Base type '{udt.base_type}' maps to CockroachDB CREATE DOMAIN"
                )
                assessment.conversion_status = "auto"
                assessment.converted_ddl = self._converter.convert_udt(udt)
            else:
                assessment.needs_review.append(f"Unmapped base type: {udt.base_type}")
                assessment.conversion_status = "partial"

            if udt.bound_rule:
                assessment.needs_review.append(
                    f"Bound rule '{udt.bound_rule}' must be converted to DOMAIN CHECK constraint"
                )
                if assessment.conversion_status == "auto":
                    assessment.conversion_status = "partial"

            report.objects.append(assessment)

    def _assess_rules(self, db_schema: DatabaseSchema, report: AssessmentReport) -> None:
        for name, rule in db_schema.rules.items():
            assessment = ObjectAssessment(name=name, object_type="rule")
            assessment.needs_review.append(
                "Sybase rule → convert to CHECK constraint or DOMAIN constraint"
            )
            assessment.conversion_status = "partial"
            report.objects.append(assessment)

    # ── Construct scanning ───────────────────────────────────

    def _scan_for_constructs(self, sql: str) -> Dict[str, List[str]]:
        """Scan SQL for unsupported and partially-supported constructs."""
        result: Dict[str, List[str]] = {"unsupported": [], "partial": []}

        if not sql:
            return result

        for pattern, message in _UNSUPPORTED_CONSTRUCTS:
            if re.search(pattern, sql, re.IGNORECASE):
                result["unsupported"].append(message)

        for pattern, message in _PARTIAL_CONSTRUCTS:
            if re.search(pattern, sql, re.IGNORECASE):
                result["partial"].append(message)

        return result

    # ── Report output ────────────────────────────────────────

    def format_report(self, report: AssessmentReport) -> str:
        """Generate a human-readable assessment report."""
        lines = [
            "=" * 70,
            "SYBASE → COCKROACHDB COMPATIBILITY ASSESSMENT",
            "=" * 70,
            f"Database: {report.database}",
            "",
            "SUMMARY",
            "-" * 40,
            f"Total objects analyzed:     {report.total_objects}",
            f"  Auto-convertible:         {report.auto_count} ({self._pct(report.auto_count, report.total_objects)})",
            f"  Partial (needs review):   {report.partial_count} ({self._pct(report.partial_count, report.total_objects)})",
            f"  Manual rewrite required:  {report.manual_count} ({self._pct(report.manual_count, report.total_objects)})",
            f"  Unsupported constructs:   {report.unsupported_count} ({self._pct(report.unsupported_count, report.total_objects)})",
            "",
        ]

        # Per-type summary
        lines.append("BY OBJECT TYPE")
        lines.append("-" * 40)
        lines.append(f"{'Type':<22} {'Total':>6} {'Auto':>6} {'Partial':>8} {'Manual':>8}")
        for obj_type, counts in sorted(report.summary.items()):
            lines.append(
                f"{obj_type:<22} {counts['total']:>6} {counts['auto']:>6} "
                f"{counts['partial']:>8} {counts['manual']:>8}"
            )
        lines.append("")

        if report.unmapped_types:
            lines.append("UNMAPPED DATA TYPES")
            lines.append("-" * 40)
            for t in report.unmapped_types:
                lines.append(f"  - {t} (will default to TEXT)")
            lines.append("")

        # Objects needing attention
        attention_objs = [
            o for o in report.objects
            if o.conversion_status in ("partial", "manual", "unsupported")
        ]
        if attention_objs:
            lines.append("OBJECTS REQUIRING ATTENTION")
            lines.append("-" * 40)
            for obj in attention_objs:
                status_label = {
                    "partial": "REVIEW",
                    "manual": "MANUAL REWRITE",
                    "unsupported": "UNSUPPORTED",
                }.get(obj.conversion_status, obj.conversion_status.upper())

                lines.append(f"\n  [{status_label}] {obj.object_type}: {obj.name}")
                for item in obj.needs_review:
                    lines.append(f"    - {item}")
                for item in obj.unsupported:
                    lines.append(f"    ! {item}")
            lines.append("")

        # Auto-converted objects summary
        auto_objs = [o for o in report.objects if o.conversion_status == "auto"]
        if auto_objs:
            lines.append(f"AUTO-CONVERTIBLE OBJECTS ({len(auto_objs)})")
            lines.append("-" * 40)
            for obj in auto_objs:
                lines.append(f"  [AUTO] {obj.object_type}: {obj.name}")
            lines.append("")

        # Recommendations
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 40)
        if report.summary.get("stored_procedure", {}).get("total", 0) > 0:
            lines.append("  1. STORED PROCEDURES: Convert T-SQL to PL/pgSQL. The toolkit generates")
            lines.append("     scaffolds but manual review is required for each procedure.")
            lines.append("     Focus on: parameter types, error handling, cursor usage, temp tables.")
        if report.summary.get("trigger", {}).get("total", 0) > 0:
            lines.append("  2. TRIGGERS: CockroachDB trigger syntax differs from Sybase.")
            lines.append("     Review trigger logic and convert to PostgreSQL trigger functions.")
        if report.summary.get("view", {}).get("partial", 0) > 0:
            lines.append("  3. VIEWS: Some views use T-SQL syntax that was auto-converted.")
            lines.append("     Verify the converted SQL executes correctly on CockroachDB.")
        if report.unmapped_types:
            lines.append(f"  4. DATA TYPES: {len(report.unmapped_types)} unmapped types found.")
            lines.append("     Review columns using these types and verify TEXT is acceptable.")
        lines.append("  5. RUN PREFLIGHT: Execute 'python migrate.py preflight' to validate")
        lines.append("     connectivity, permissions, and environment before migration.")
        lines.append("  6. TEST FIRST: Migrate a small subset of tables to validate the process")
        lines.append("     end-to-end before running the full migration.")
        lines.append("")

        return "\n".join(lines)

    def save_report(self, report: AssessmentReport, output_dir: str) -> Tuple[str, str]:
        """Save assessment report as text and JSON files."""
        os.makedirs(output_dir, exist_ok=True)

        # Text report
        txt_path = os.path.join(output_dir, "assessment_report.txt")
        with open(txt_path, "w") as fh:
            fh.write(self.format_report(report))
        logger.info("Assessment report saved to %s", txt_path)

        # JSON summary
        json_path = os.path.join(output_dir, "assessment_summary.json")
        summary = {
            "database": report.database,
            "total_objects": report.total_objects,
            "auto_count": report.auto_count,
            "partial_count": report.partial_count,
            "manual_count": report.manual_count,
            "unsupported_count": report.unsupported_count,
            "by_type": report.summary,
            "unmapped_types": report.unmapped_types,
            "objects": [
                {
                    "name": o.name,
                    "type": o.object_type,
                    "status": o.conversion_status,
                    "review_items": o.needs_review,
                    "unsupported_items": o.unsupported,
                }
                for o in report.objects
                if o.conversion_status != "auto"  # Only include non-auto for brevity
            ],
        }
        with open(json_path, "w") as fh:
            json.dump(summary, fh, indent=2)
        logger.info("Assessment summary saved to %s", json_path)

        # Save converted DDL for all auto/partial objects
        ddl_path = os.path.join(output_dir, "converted_schema.sql")
        ddl_parts = []
        for obj in report.objects:
            if obj.converted_ddl:
                ddl_parts.append(f"-- {obj.object_type}: {obj.name} [{obj.conversion_status}]")
                ddl_parts.append(obj.converted_ddl)
                ddl_parts.append("")
        with open(ddl_path, "w") as fh:
            fh.write("\n".join(ddl_parts))
        logger.info("Converted DDL saved to %s", ddl_path)

        return txt_path, json_path

    @staticmethod
    def _pct(part: int, total: int) -> str:
        if total == 0:
            return "0%"
        return f"{part * 100 / total:.0f}%"
