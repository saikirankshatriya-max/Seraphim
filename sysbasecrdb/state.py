"""Thread-safe, JSON-persisted migration state machine with resume support."""

import json
import logging
import os
import threading
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger("sysbasecrdb.state")


class MigrationPhase(Enum):
    INIT = "init"
    SCHEMA_EXTRACTED = "schema_extracted"
    CDC_INSTALLED = "cdc_installed"
    SCHEMA_CREATED = "schema_created"
    EXPORTING = "exporting"
    EXPORT_DONE = "export_done"
    IMPORTING = "importing"
    IMPORT_DONE = "import_done"
    CDC_REPLAYING = "cdc_replaying"
    VALIDATED = "validated"
    FKS_APPLIED = "fks_applied"
    CLEANUP = "cleanup"
    COMPLETE = "complete"


class TableStatus(Enum):
    PENDING = "pending"
    EXPORTING = "exporting"
    EXPORTED = "exported"
    IMPORTING = "importing"
    IMPORTED = "imported"
    CDC_REPLAYING = "cdc_replaying"
    VALIDATED = "validated"
    COMPLETE = "complete"
    ERROR = "error"


# Allowed phase transitions (current -> set of valid next phases)
_PHASE_ORDER = [p for p in MigrationPhase]


class MigrationState:
    """Thread-safe, JSON-persisted migration state.

    State file is written atomically (write-to-tmp then os.replace)
    so a crash mid-save never corrupts the file.
    """

    def __init__(self, state_file: str):
        self._path = state_file
        self._lock = threading.Lock()
        self._state = self._load()

    # ── persistence ──────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self._path):
            logger.info("Resuming from existing state file: %s", self._path)
            with open(self._path, "r") as fh:
                return json.load(fh)
        logger.info("No existing state file — starting fresh")
        return {
            "phase": MigrationPhase.INIT.value,
            "started_at": datetime.utcnow().isoformat(),
            "updated_at": None,
            "tables": {},
        }

    def _save(self) -> None:
        """Atomically persist state to disk."""
        self._state["updated_at"] = datetime.utcnow().isoformat()
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp = self._path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self._state, fh, indent=2)
        os.replace(tmp, self._path)

    # ── overall phase ────────────────────────────────────────

    def get_phase(self) -> MigrationPhase:
        with self._lock:
            return MigrationPhase(self._state["phase"])

    def set_phase(self, phase: MigrationPhase) -> None:
        with self._lock:
            logger.info("Migration phase: %s -> %s", self._state["phase"], phase.value)
            self._state["phase"] = phase.value
            self._save()

    # ── per-table status ─────────────────────────────────────

    def init_table(self, table: str) -> None:
        """Register a table in the state if not already present."""
        with self._lock:
            if table not in self._state["tables"]:
                self._state["tables"][table] = {
                    "status": TableStatus.PENDING.value,
                    "last_cdc_seq": None,
                    "error": None,
                    "updated_at": datetime.utcnow().isoformat(),
                }
                self._save()

    def get_table_status(self, table: str) -> Optional[str]:
        with self._lock:
            entry = self._state["tables"].get(table, {})
            return entry.get("status")

    def set_table_status(self, table: str, status: str) -> None:
        with self._lock:
            if table not in self._state["tables"]:
                self._state["tables"][table] = {}
            self._state["tables"][table]["status"] = status
            if status != TableStatus.ERROR.value:
                self._state["tables"][table]["error"] = None
            self._state["tables"][table]["updated_at"] = datetime.utcnow().isoformat()
            self._save()

    def set_table_error(self, table: str, error: str) -> None:
        with self._lock:
            if table not in self._state["tables"]:
                self._state["tables"][table] = {}
            self._state["tables"][table]["status"] = TableStatus.ERROR.value
            self._state["tables"][table]["error"] = error
            self._state["tables"][table]["updated_at"] = datetime.utcnow().isoformat()
            logger.error("Table %s entered error state: %s", table, error)
            self._save()

    # ── CDC tracking ─────────────────────────────────────────

    def get_last_cdc_seq(self, table: str) -> Optional[int]:
        with self._lock:
            return self._state["tables"].get(table, {}).get("last_cdc_seq")

    def set_last_cdc_seq(self, table: str, seq: int) -> None:
        with self._lock:
            if table not in self._state["tables"]:
                self._state["tables"][table] = {}
            self._state["tables"][table]["last_cdc_seq"] = seq
            self._save()

    # ── bulk queries ─────────────────────────────────────────

    def get_all_table_statuses(self) -> Dict[str, str]:
        with self._lock:
            return {
                t: v.get("status", "unknown")
                for t, v in self._state["tables"].items()
            }

    def get_error_tables(self) -> Dict[str, str]:
        with self._lock:
            return {
                t: v.get("error", "")
                for t, v in self._state["tables"].items()
                if v.get("status") == TableStatus.ERROR.value
            }

    def get_tables_in_status(self, status: str) -> list:
        with self._lock:
            return [
                t
                for t, v in self._state["tables"].items()
                if v.get("status") == status
            ]

    def reset_table_to(self, table: str, status: str) -> None:
        """Reset a table (e.g. from error) back to a given status for retry."""
        with self._lock:
            if table in self._state["tables"]:
                self._state["tables"][table]["status"] = status
                self._state["tables"][table]["error"] = None
                self._state["tables"][table]["updated_at"] = datetime.utcnow().isoformat()
                self._save()
                logger.info("Table %s reset to %s", table, status)

    # ── summary ──────────────────────────────────────────────

    def summary(self) -> dict:
        """Return a human-readable summary of the migration state."""
        with self._lock:
            statuses = {}
            for v in self._state["tables"].values():
                s = v.get("status", "unknown")
                statuses[s] = statuses.get(s, 0) + 1
            return {
                "phase": self._state["phase"],
                "started_at": self._state.get("started_at"),
                "updated_at": self._state.get("updated_at"),
                "total_tables": len(self._state["tables"]),
                "status_counts": statuses,
                "error_count": statuses.get("error", 0),
            }
