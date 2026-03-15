# Seraphim Roadmap

This roadmap is intentionally narrow: production-grade Sybase ASE to CockroachDB migration only.

## Goal

Bring Seraphim to a quality bar comparable to mature migration tools for its specific domain:

- correct and resumable migrations
- deterministic cutover behavior
- operator-safe execution
- strong validation and recovery paths
- packaging, tests, and release discipline

## Assumptions

- Scope is limited to Sybase ASE -> CockroachDB
- One senior engineer is driving implementation
- A DBA or infra owner is available for environment validation and test data setup
- Target duration is 12 weeks

## P0

P0 is about correctness and trust. No new feature work should land ahead of this.

### Week 1: Test Harness And Quality Baseline

Milestone:
- Establish a repeatable engineering baseline before changing migration behavior

Work:
- Add `pytest` and test layout
- Add unit-test coverage for config loading, state transitions, identifier quoting, and CSV escaping
- Add lint/format tooling and a local CI entrypoint
- Add issue templates for regression, data mismatch, and recovery failure

Acceptance criteria:
- Tests run in one command
- CI can run lint plus unit tests
- At least one test exists for each critical support module
- No behavior changes yet

### Week 2: State Machine Hardening

Milestone:
- Migration phases become explicit, valid, and fail-closed

Work:
- Enforce legal phase transitions in the state layer
- Prevent phase advancement when required table-level work failed
- Add explicit terminal states for partial failure and blocked cutover
- Make resume behavior deterministic from every persisted phase

Acceptance criteria:
- Invalid transitions are rejected
- Failed tables block phase completion where required
- Resume tests pass from each persisted phase
- State file format is versioned

### Week 3: CDC Replay Correctness

Milestone:
- CDC replay can no longer report success when replay actually failed

Work:
- Make worker failures block replay completion
- Add deterministic checkpoint behavior per table
- Add replay retries for transient failures
- Add tests for duplicate CDC application, out-of-order interruption, and replay restart

Acceptance criteria:
- Any replay worker failure causes the run to fail
- Replay checkpoints are monotonic and persisted
- Restart after interruption continues from last safe checkpoint
- CDC tests cover insert, update, and delete reapplication

### Week 4: Validation Becomes Authoritative

Milestone:
- Validation becomes a release gate, not a best-effort report

Work:
- Separate row count, aggregates, checksum, and sample validation flags
- Make sample mismatches fail validation
- Tighten numeric comparison rules and reporting
- Add machine-readable validation output

Acceptance criteria:
- Any configured validation failure fails the run
- Reports clearly show blocking vs informational findings
- Validation tests include mismatched rows, mismatched aggregates, and encoding differences
- Validation output is available in text and JSON

## P1

P1 is about end-to-end operator readiness for real migrations.

### Week 5: Resume Determinism And Artifact Integrity

Milestone:
- Cached state and schema artifacts are complete enough to guarantee consistent resumes

Work:
- Preserve computed columns, defaults, checks, indexes, foreign keys, and metadata in cached schemas
- Add artifact manifest and checksum tracking for exported files
- Block false-success cases such as empty import directories
- Add explicit verification after export and import steps

Acceptance criteria:
- Fresh run and resumed run produce the same schema model
- Tables with missing export artifacts cannot be marked imported
- File manifests are persisted and verified
- Resume tests cover export interruption and import interruption

### Week 6: SQL Rendering And Identifier Safety

Milestone:
- SQL generation becomes safe for real-world table and column names

Work:
- Centralize Sybase and Cockroach identifier rendering
- Remove raw interpolation for identifiers where avoidable
- Support owner-qualified names and reserved words correctly
- Add golden tests for mixed case, spaces, symbols, and quoted identifiers

Acceptance criteria:
- Identifier tests cover unusual but valid schema names
- No lowercasing or quoting regressions for Cockroach DDL/DML generation
- SQL generation helpers are used by extraction, export, import, replay, and validation paths

### Week 7: Snapshot Plus CDC Cutover Protocol

Milestone:
- Bulk export and CDC replay work as a coherent cutover system

Work:
- Define initial snapshot watermark capture
- Add final cutover workflow: write freeze, drain to watermark, final validation, release
- Persist per-table and global cutover status
- Add operator prompts and runbook steps for cutover

Acceptance criteria:
- Cutover protocol is documented and implemented
- Final catch-up cannot complete without explicit write-freeze confirmation
- Drain-to-watermark is testable
- Final validation runs against the cutover checkpoint

### Week 8: Sybase Type And Schema Coverage

Milestone:
- The conversion layer is reliable for the supported Sybase feature set

Work:
- Harden mappings for numeric, money, datetime, binary, LOB, identity, and rowversion-like types
- Improve default-expression conversion
- Improve computed-column and check-constraint conversion behavior
- Add compatibility fixtures for representative Sybase schemas

Acceptance criteria:
- Supported type matrix is documented
- Golden DDL tests cover all supported types
- Unsupported constructs are reported explicitly, not silently degraded
- Assessment output clearly separates auto-convertible from manual items

### Week 9: Operator Workflow And Reporting

Milestone:
- The tool is usable by an operator without reading source code

Work:
- Refine CLI commands around `doctor`, `plan`, `run`, `resume`, `status`, `cutover`, `validate`, and `cleanup`
- Add structured logs, run IDs, per-table progress, lag, throughput, and ETA
- Improve error categories and remediation guidance
- Add stable JSON report formats for preflight, assessment, and validation

Acceptance criteria:
- A full dry-run workflow is documented and executable
- `status` is useful during long-running migrations
- Logs and reports are sufficient for on-call debugging
- Operators can identify blocked tables and next actions quickly

## P2

P2 is about shipping, repeatability, and long-run confidence.

### Week 10: Integration Environments And Failure Injection

Milestone:
- Real environment testing exists beyond local unit tests

Work:
- Create integration test environments for Sybase ASE and CockroachDB
- Add seeded schemas and datasets for small, medium, and large runs
- Add failure-injection tests for network loss, disk pressure, interrupted export, interrupted replay, and partial import

Acceptance criteria:
- Integration suite runs against controlled environments
- At least one end-to-end migration test passes
- At least one interrupted migration test proves safe resume
- Regression fixtures are preserved for future runs

### Week 11: Packaging And Release Discipline

Milestone:
- Seraphim becomes a releasable tool, not just a codebase

Work:
- Add `pyproject.toml` and console entrypoint
- Pin dependencies and define supported Python versions
- Add semantic versioning, changelog, and upgrade notes for config/state changes
- Add release build checks and smoke tests

Acceptance criteria:
- Tool installs in a clean environment with one documented flow
- Version information is exposed in the CLI
- Release process is documented
- Upgrade notes exist for any state or config format change

### Week 12: Production Readiness Review

Milestone:
- Final go/no-go review for first production migration

Work:
- Run one controlled pilot migration
- Capture throughput, lag, validation quality, and operator feedback
- Close remaining P0/P1 regressions
- Publish the supported matrix, runbook, and known limitations

Acceptance criteria:
- Pilot migration completes with documented outcomes
- Known limitations are published
- Recovery and rollback procedures are tested and written down
- First production candidate release is tagged

## Deliverables By Priority

### P0 Deliverables

- Versioned state machine
- Blocking validation
- Correct CDC replay failure handling
- Resume-safe migration artifacts
- Unit test foundation

### P1 Deliverables

- Safe SQL rendering layer
- Deterministic cutover flow
- Hardened Sybase type coverage
- Operator-grade CLI and reporting

### P2 Deliverables

- Integration suite
- Packaged CLI release
- Runbooks and release process
- Pilot migration evidence

## Exit Criteria

Seraphim is ready for serious production use when all of the following are true:

- every supported migration path is covered by automated tests
- interrupted runs resume safely without manual state surgery
- validation failures block completion
- cutover cannot silently succeed after CDC errors
- unsupported Sybase constructs are reported explicitly
- operator documentation is sufficient for a non-author developer to run the tool
- the packaged CLI can be installed and used in a clean environment
