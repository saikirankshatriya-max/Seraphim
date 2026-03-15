"""Command-line interface for the Sybase-to-CockroachDB migration toolkit."""

import argparse
import json
import sys
import logging

from .config import load_config
from .logger import setup_logging
from .orchestrator import MigrationOrchestrator
from .state import MigrationState

logger = logging.getLogger("sysbasecrdb.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrate",
        description="Sybase ASE to CockroachDB Migration Toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Full migration pipeline
  python migrate.py -c config/migration.yaml run

  # Resume from a specific phase
  python migrate.py -c config/migration.yaml run --from-phase importing

  # Run individual steps
  python migrate.py -c config/migration.yaml extract-schema
  python migrate.py -c config/migration.yaml install-cdc
  python migrate.py -c config/migration.yaml create-schema
  python migrate.py -c config/migration.yaml export
  python migrate.py -c config/migration.yaml import
  python migrate.py -c config/migration.yaml cdc-replay --continuous
  python migrate.py -c config/migration.yaml validate
  python migrate.py -c config/migration.yaml apply-fks
  python migrate.py -c config/migration.yaml cleanup

  # Check progress
  python migrate.py -c config/migration.yaml status

  # Retry failed tables
  python migrate.py -c config/migration.yaml retry-errors
""",
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to migration.yaml"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Migration subcommand")

    # Full pipeline
    sub_run = subparsers.add_parser("run", help="Run the full migration pipeline")
    sub_run.add_argument(
        "--from-phase",
        help="Resume from a specific phase (e.g. 'importing', 'cdc_replaying')",
    )

    # Individual steps
    subparsers.add_parser("extract-schema", help="Extract schema from Sybase")
    subparsers.add_parser("install-cdc", help="Install CDC triggers on Sybase")
    subparsers.add_parser("create-schema", help="Create tables on CockroachDB")
    subparsers.add_parser("export", help="Export data from Sybase via BCP")
    subparsers.add_parser("import", help="Import data into CockroachDB")

    sub_cdc = subparsers.add_parser("cdc-replay", help="Replay CDC log changes")
    sub_cdc.add_argument(
        "--continuous", action="store_true",
        help="Run continuously until cutover threshold is met",
    )

    subparsers.add_parser("validate", help="Validate data between source and target")
    subparsers.add_parser("apply-fks", help="Apply foreign keys on CockroachDB")
    subparsers.add_parser("cleanup", help="Remove CDC triggers and log tables from Sybase")

    # Pre-migration analysis commands
    subparsers.add_parser("preflight", help="Run pre-flight environment checks")
    subparsers.add_parser("assess", help="Run compatibility assessment (analyzes what converts)")

    # Status
    subparsers.add_parser("status", help="Show migration status")

    # Retry
    sub_retry = subparsers.add_parser("retry-errors", help="Retry tables in error state")
    sub_retry.add_argument(
        "--tables", nargs="*", help="Specific tables to retry (default: all errored)"
    )
    sub_retry.add_argument(
        "--reset-to",
        default="pending",
        help="Reset errored tables to this status (default: pending)",
    )

    return parser


def cmd_status(config):
    """Show current migration status."""
    state = MigrationState(config.state_file)
    summary = state.summary()

    print(f"\nMigration Phase: {summary['phase']}")
    print(f"Started:         {summary['started_at']}")
    print(f"Last Updated:    {summary['updated_at']}")
    print(f"Total Tables:    {summary['total_tables']}")
    print(f"\nTable Status Breakdown:")
    for status, count in sorted(summary["status_counts"].items()):
        print(f"  {status:20s} {count}")

    if summary["error_count"] > 0:
        print(f"\nTables with Errors ({summary['error_count']}):")
        for table, error in state.get_error_tables().items():
            print(f"  {table}: {error}")
    print()


def cmd_retry_errors(config, tables=None, reset_to="pending"):
    """Reset errored tables for retry."""
    state = MigrationState(config.state_file)
    error_tables = state.get_error_tables()

    if not error_tables:
        print("No tables in error state.")
        return

    if tables:
        to_retry = {t: e for t, e in error_tables.items() if t in tables}
    else:
        to_retry = error_tables

    for table in to_retry:
        state.reset_table_to(table, reset_to)
        print(f"Reset {table} to '{reset_to}'")

    print(f"\n{len(to_retry)} tables reset. Run the appropriate step to retry.")


def cmd_single_step(config, step_name):
    """Run a single step of the migration."""
    orch = MigrationOrchestrator(config)

    # Load schemas for all steps except extract-schema
    if step_name != "extract-schema":
        orch._load_cached_schemas()

    step_map = {
        "extract-schema": orch._step_extract_schema,
        "install-cdc":    orch._step_install_cdc,
        "create-schema":  orch._step_create_schema,
        "export":         orch._step_export,
        "import":         orch._step_import,
        "validate":       orch._step_validate,
        "apply-fks":      orch._step_apply_fks,
        "cleanup":        orch._step_cleanup,
    }

    if step_name not in step_map:
        print(f"Unknown step: {step_name}")
        sys.exit(1)

    step_map[step_name]()


def cmd_cdc_replay(config, continuous=False):
    """Run CDC replay."""
    from .cdc_replay import CDCReplayer
    from .connections import CockroachConnection, SybaseConnection

    orch = MigrationOrchestrator(config)
    orch._load_cached_schemas()

    sybase = SybaseConnection(config.source)
    crdb = CockroachConnection(config.target)
    state = MigrationState(config.state_file)

    replayer = CDCReplayer(config, state, sybase, crdb, orch._schemas)

    imported = state.get_tables_in_status("imported")
    if not imported:
        imported = list(orch._schemas.keys())

    replayer.replay_loop(imported, until_cutover=continuous)


def cmd_preflight(config):
    """Run pre-flight environment checks."""
    from .preflight import PreflightChecker

    checker = PreflightChecker(config)
    report = checker.run_all_checks()
    print(report.format_report())

    # Save report
    import os
    report_path = os.path.join(config.log_dir, "preflight_report.txt")
    os.makedirs(config.log_dir, exist_ok=True)
    with open(report_path, "w") as fh:
        fh.write(report.format_report())
    print(f"\nReport saved to: {report_path}")

    if not report.passed:
        sys.exit(1)


def cmd_assess(config):
    """Run compatibility assessment."""
    from .assessment import CompatibilityAssessor

    assessor = CompatibilityAssessor(config)
    report = assessor.run_assessment()
    formatted = assessor.format_report(report)
    print(formatted)

    txt_path, json_path = assessor.save_report(report, config.log_dir)
    print(f"\nDetailed report: {txt_path}")
    print(f"JSON summary:    {json_path}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    config = load_config(args.config)
    setup_logging(config.log_dir, config.log_level, args.verbose)

    try:
        if args.command == "run":
            orch = MigrationOrchestrator(config)
            orch.run(start_from=getattr(args, "from_phase", None))
        elif args.command == "status":
            cmd_status(config)
        elif args.command == "preflight":
            cmd_preflight(config)
        elif args.command == "assess":
            cmd_assess(config)
        elif args.command == "retry-errors":
            cmd_retry_errors(
                config,
                tables=getattr(args, "tables", None),
                reset_to=getattr(args, "reset_to", "pending"),
            )
        elif args.command == "cdc-replay":
            cmd_cdc_replay(config, continuous=getattr(args, "continuous", False))
        else:
            cmd_single_step(config, args.command)
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logger.error("Migration failed: %s", exc, exc_info=True)
        sys.exit(1)
