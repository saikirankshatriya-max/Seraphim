"""Configuration loader: reads migration.yaml, interpolates env vars, returns typed config."""

import os
import re
import yaml
import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger("sysbasecrdb.config")

ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _interpolate_env(value: str) -> str:
    """Replace ${VAR_NAME} tokens with their environment variable values."""

    def _replace(match: re.Match) -> str:
        var = match.group(1)
        val = os.environ.get(var)
        if val is None:
            raise EnvironmentError(f"Environment variable ${{{var}}} is not set")
        return val

    return ENV_VAR_PATTERN.sub(_replace, value)


def _interpolate_recursive(obj):
    """Walk a nested dict/list and interpolate env vars in all string values."""
    if isinstance(obj, str):
        return _interpolate_env(obj)
    if isinstance(obj, dict):
        return {k: _interpolate_recursive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_recursive(v) for v in obj]
    return obj


@dataclass
class SourceConfig:
    driver: str
    host: str
    port: int
    database: str
    username: str
    password: str
    bcp_path: str
    isql_path: str
    charset: str = "utf8"
    odbc_dsn: Optional[str] = None


@dataclass
class TargetConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "verify-full"
    sslrootcert: Optional[str] = None
    import_method: str = "userfile"
    userfile_upload_cmd: str = "cockroach userfile upload"


@dataclass
class ExportConfig:
    delimiter: str = "|"
    row_terminator: str = "\\n"
    null_marker: str = "\\N"
    bcp_batch_size: int = 10000
    bcp_packet_size: int = 8192


@dataclass
class ImportConfig:
    copy_threshold: int = 50_000_000  # bytes — files below this use COPY FROM


@dataclass
class CDCConfig:
    poll_interval_seconds: int = 2
    batch_size: int = 5000
    lag_threshold: int = 100


@dataclass
class ValidationConfig:
    row_count: bool = True
    checksum: bool = True
    sample_rate: float = 1.0


@dataclass
class MigrationConfig:
    source: SourceConfig
    target: TargetConfig
    export_dir: str
    state_file: str
    log_dir: str
    log_level: str

    table_include: List[str] = field(default_factory=lambda: ["*"])
    table_exclude: List[str] = field(default_factory=list)

    export_workers: int = 8
    import_workers: int = 4
    cdc_replay_workers: int = 4

    large_table_threshold: int = 1_000_000
    chunk_size: int = 500_000
    pk_sampling_limit: int = 10_000

    export: ExportConfig = field(default_factory=ExportConfig)
    import_cfg: ImportConfig = field(default_factory=ImportConfig)
    cdc: CDCConfig = field(default_factory=CDCConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)


def load_config(path: str) -> MigrationConfig:
    """Load a YAML config file and return a fully typed MigrationConfig."""
    logger.info("Loading configuration from %s", path)
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)

    raw = _interpolate_recursive(raw)

    src = raw["source"]
    tgt = raw["target"]
    mig = raw["migration"]

    source_cfg = SourceConfig(
        driver=src["driver"],
        host=src["host"],
        port=int(src["port"]),
        database=src["database"],
        username=src["username"],
        password=src["password"],
        bcp_path=src.get("bcp_path", "bcp"),
        isql_path=src.get("isql_path", "isql"),
        charset=src.get("charset", "utf8"),
        odbc_dsn=src.get("odbc_dsn"),
    )

    target_cfg = TargetConfig(
        host=tgt["host"],
        port=int(tgt["port"]),
        database=tgt["database"],
        username=tgt["username"],
        password=tgt["password"],
        sslmode=tgt.get("sslmode", "verify-full"),
        sslrootcert=tgt.get("sslrootcert"),
        import_method=tgt.get("import_method", "userfile"),
        userfile_upload_cmd=tgt.get("userfile_upload_cmd", "cockroach userfile upload"),
    )

    par = mig.get("parallelism", {})
    chunking = mig.get("chunking", {})
    exp = mig.get("export", {})
    imp = mig.get("import", {})
    cdc = mig.get("cdc", {})
    val = mig.get("validation", {})
    tables = mig.get("tables", {})

    export_cfg = ExportConfig(
        delimiter=exp.get("delimiter", "|"),
        row_terminator=exp.get("row_terminator", "\\n"),
        null_marker=exp.get("null_marker", "\\N"),
        bcp_batch_size=int(exp.get("bcp_batch_size", 10000)),
        bcp_packet_size=int(exp.get("bcp_packet_size", 8192)),
    )

    import_cfg = ImportConfig(
        copy_threshold=int(imp.get("copy_threshold", 50_000_000)),
    )

    cdc_cfg = CDCConfig(
        poll_interval_seconds=int(cdc.get("poll_interval_seconds", 2)),
        batch_size=int(cdc.get("batch_size", 5000)),
        lag_threshold=int(cdc.get("lag_threshold", 100)),
    )

    validation_cfg = ValidationConfig(
        row_count=val.get("row_count", True),
        checksum=val.get("checksum", True),
        sample_rate=float(val.get("sample_rate", 1.0)),
    )

    return MigrationConfig(
        source=source_cfg,
        target=target_cfg,
        export_dir=mig["export_dir"],
        state_file=mig["state_file"],
        log_dir=mig["log_dir"],
        log_level=mig.get("log_level", "INFO"),
        table_include=tables.get("include", ["*"]),
        table_exclude=tables.get("exclude", []),
        export_workers=int(par.get("export_workers", 8)),
        import_workers=int(par.get("import_workers", 4)),
        cdc_replay_workers=int(par.get("cdc_replay_workers", 4)),
        large_table_threshold=int(chunking.get("large_table_threshold", 1_000_000)),
        chunk_size=int(chunking.get("chunk_size", 500_000)),
        pk_sampling_limit=int(chunking.get("pk_sampling_limit", 10_000)),
        export=export_cfg,
        import_cfg=import_cfg,
        cdc=cdc_cfg,
        validation=validation_cfg,
    )
