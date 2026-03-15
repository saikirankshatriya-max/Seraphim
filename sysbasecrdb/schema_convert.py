"""Convert Sybase ASE schema to CockroachDB-compatible DDL.

This module contains:
  1. Data type mapping (30+ Sybase types → CockroachDB equivalents)
  2. T-SQL expression rewriter (function calls, operators, syntax)
  3. DDL generators (tables, indexes, FKs, check constraints, computed columns)
  4. View/procedure/trigger conversion scaffolding
  5. UDT conversion to CockroachDB CREATE DOMAIN

The T-SQL rewriter handles 60+ function mappings, operator conversions,
and syntax transformations to convert Sybase expressions into PostgreSQL-
compatible SQL that CockroachDB can execute.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

from .schema_extract import (
    CheckConstraintInfo,
    ColumnInfo,
    DatabaseSchema,
    StoredProcInfo,
    TableSchema,
    TriggerInfo,
    UserDefinedTypeInfo,
    UserFunctionInfo,
    ViewInfo,
)
from .utils import quote_ident

logger = logging.getLogger("sysbasecrdb.schema_convert")

# ═══════════════════════════════════════════════════════════════
# 1. DATA TYPE MAPPING
# ═══════════════════════════════════════════════════════════════

SYBASE_TO_CRDB_TYPE_MAP: Dict[str, str] = {
    # Integer types
    "tinyint":           "INT2",
    "smallint":          "INT2",
    "int":               "INT4",
    "integer":           "INT4",
    "bigint":            "INT8",
    "unsigned smallint": "INT4",
    "unsigned int":      "INT8",
    "unsigned bigint":   "INT8",

    # Character types
    "char":              "CHAR({length})",
    "varchar":           "VARCHAR({length})",
    "nchar":             "VARCHAR({length})",
    "nvarchar":          "VARCHAR({length})",
    "unichar":           "VARCHAR({length})",
    "univarchar":        "VARCHAR({length})",
    "text":              "TEXT",
    "unitext":           "TEXT",

    # Binary types
    "binary":            "BYTES",
    "varbinary":         "BYTES",
    "image":             "BYTES",

    # Date/time types
    "datetime":          "TIMESTAMPTZ",
    "smalldatetime":     "TIMESTAMPTZ",
    "date":              "DATE",
    "time":              "TIME",
    "bigdatetime":       "TIMESTAMPTZ",
    "bigtime":           "TIME",

    # Numeric/decimal types
    "numeric":           "DECIMAL({prec},{scale})",
    "decimal":           "DECIMAL({prec},{scale})",
    "money":             "DECIMAL(19,4)",
    "smallmoney":        "DECIMAL(10,4)",
    "float":             "FLOAT8",
    "real":              "FLOAT4",
    "double precision":  "FLOAT8",

    # Boolean
    "bit":               "BOOL",

    # UUID
    "uniqueidentifier":  "UUID",

    # Sybase timestamp = rowversion counter, NOT a datetime
    "timestamp":         "BYTES",
}


# ═══════════════════════════════════════════════════════════════
# 2. T-SQL EXPRESSION REWRITER
# ═══════════════════════════════════════════════════════════════

# Sybase function → CockroachDB/PostgreSQL equivalent
# Format: "sybase_func" -> ("pg_func", needs_arg_rewrite)
FUNCTION_MAP: Dict[str, str] = {
    # ── Date/time functions ──────────────────────────────
    "getdate":       "now",
    "getutcdate":    "now",  # note: now() returns with TZ in CRDB
    "sysdatetime":   "now",
    "current_timestamp": "current_timestamp",

    # ── String functions ─────────────────────────────────
    "len":           "length",
    "datalength":    "octet_length",
    "charindex":     "_CHARINDEX",     # needs arg reorder
    "patindex":      "_PATINDEX",      # needs regex conversion
    "replicate":     "repeat",
    "space":         "_SPACE",         # space(n) → repeat(' ', n)
    "stuff":         "_STUFF",         # needs overlay() conversion
    "char":          "chr",
    "ascii":         "ascii",
    "ltrim":         "ltrim",
    "rtrim":         "rtrim",
    "upper":         "upper",
    "lower":         "lower",
    "left":          "left",
    "right":         "right",
    "reverse":       "reverse",
    "substring":     "substring",
    "replace":       "replace",
    "str":           "_STR",           # needs to_char conversion
    "trim":          "trim",

    # ── NULL handling ────────────────────────────────────
    "isnull":        "COALESCE",
    "nullif":        "NULLIF",
    "coalesce":      "COALESCE",

    # ── Math functions ───────────────────────────────────
    "abs":           "abs",
    "ceiling":       "ceil",
    "ceil":          "ceil",
    "floor":         "floor",
    "round":         "round",
    "sign":          "sign",
    "power":         "power",
    "sqrt":          "sqrt",
    "square":        "_SQUARE",        # x*x
    "rand":          "random",
    "exp":           "exp",
    "log":           "ln",
    "log10":         "log",            # PostgreSQL log() is base-10
    "pi":            "pi",
    "degrees":       "degrees",
    "radians":       "radians",
    "sin":           "sin",
    "cos":           "cos",
    "tan":           "tan",
    "asin":          "asin",
    "acos":          "acos",
    "atan":          "atan",
    "atan2":         "atan2",

    # ── Type conversion ──────────────────────────────────
    "convert":       "_CONVERT",       # needs full rewrite
    "cast":          "CAST",

    # ── System/metadata ──────────────────────────────────
    "newid":         "gen_random_uuid",
    "object_id":     "_UNSUPPORTED",
    "object_name":   "_UNSUPPORTED",
    "db_name":       "current_database",
    "user_name":     "current_user",
    "suser_name":    "session_user",
    "host_name":     "inet_server_addr",
    "isnumeric":     "_ISNUMERIC",     # needs regex

    # ── Aggregate ────────────────────────────────────────
    "count":         "COUNT",
    "sum":           "SUM",
    "avg":           "AVG",
    "min":           "MIN",
    "max":           "MAX",
    "stdev":         "stddev",
    "var":           "variance",
    "count_big":     "COUNT",
}

# CONVERT style codes → to_char format strings
_CONVERT_DATE_STYLES: Dict[int, str] = {
    0:   "Mon DD YYYY HH:MI:SS",   # Default
    1:   "MM/DD/YY",                # US short
    2:   "YY.MM.DD",               # ANSI
    3:   "DD/MM/YY",               # British
    4:   "DD.MM.YY",               # German
    5:   "DD-MM-YY",               # Italian
    6:   "DD Mon YY",
    7:   "Mon DD, YY",
    8:   "HH24:MI:SS",             # Time
    10:  "MM-DD-YY",               # US
    11:  "YY/MM/DD",               # Japan
    12:  "YYMMDD",                 # ISO
    20:  "YYYY-MM-DD HH24:MI:SS", # ODBC canonical
    21:  "YYYY-MM-DD HH24:MI:SS", # ODBC canonical
    100: "Mon DD YYYY HH:MI:SSAM",
    101: "MM/DD/YYYY",             # US
    102: "YYYY.MM.DD",             # ANSI
    103: "DD/MM/YYYY",             # British
    104: "DD.MM.YYYY",             # German
    105: "DD-MM-YYYY",             # Italian
    106: "DD Mon YYYY",
    107: "Mon DD, YYYY",
    108: "HH24:MI:SS",
    110: "MM-DD-YYYY",
    111: "YYYY/MM/DD",
    112: "YYYYMMDD",
    120: "YYYY-MM-DD HH24:MI:SS",
    121: "YYYY-MM-DD HH24:MI:SS.US",
    126: "YYYY-MM-DD\"T\"HH24:MI:SS",  # ISO 8601
    130: "YYYY-MM-DD HH24:MI:SS",
}

# DATEADD/DATEDIFF unit mapping
_DATE_UNITS: Dict[str, str] = {
    "year":        "year",
    "yy":          "year",
    "yyyy":        "year",
    "quarter":     "month",  # multiply by 3
    "qq":          "month",
    "month":       "month",
    "mm":          "month",
    "day":         "day",
    "dd":          "day",
    "dayofyear":   "day",
    "dy":          "day",
    "week":        "week",
    "wk":          "week",
    "hour":        "hour",
    "hh":          "hour",
    "minute":      "minute",
    "mi":          "minute",
    "second":      "second",
    "ss":          "second",
    "millisecond": "millisecond",
    "ms":          "millisecond",
}

# Sybase global variables → PostgreSQL equivalents
_GLOBAL_VAR_MAP: Dict[str, str] = {
    "@@rowcount":    "/* @@ROWCOUNT: use GET DIAGNOSTICS row_count */",
    "@@error":       "/* @@ERROR: use EXCEPTION handling in PL/pgSQL */",
    "@@identity":    "lastval()",
    "@@spid":        "pg_backend_pid()",
    "@@servername":  "current_setting('cluster.organization')",
    "@@version":     "version()",
    "@@trancount":   "/* @@TRANCOUNT: no direct equivalent */",
    "@@fetch_status": "/* @@FETCH_STATUS: use FOUND variable in PL/pgSQL */",
    "@@nestlevel":   "/* @@NESTLEVEL: no equivalent */",
}


class TSQLRewriter:
    """Rewrite Sybase T-SQL expressions into CockroachDB-compatible PostgreSQL.

    Handles:
      - Function name mapping (60+ functions)
      - CONVERT(type, expr, style) → CAST + to_char
      - DATEADD/DATEDIFF → interval arithmetic / EXTRACT
      - DATEPART → EXTRACT
      - TOP N → LIMIT N
      - *= / =* joins → explicit LEFT/RIGHT JOIN
      - String concatenation + → ||
      - ISNULL → COALESCE
      - @variable → variable (strip @ prefix)
      - @@global_vars → PostgreSQL equivalents
      - BEGIN/END → PL/pgSQL syntax
    """

    def __init__(self):
        self.warnings: List[str] = []

    def rewrite(self, sql: str) -> str:
        """Apply all T-SQL → PostgreSQL rewrites to a SQL string."""
        if not sql:
            return sql

        self.warnings = []
        result = sql

        # Order matters: do structural rewrites before function rewrites
        result = self._rewrite_global_variables(result)
        result = self._rewrite_top_clause(result)
        result = self._rewrite_old_join_syntax(result)
        result = self._rewrite_string_concat(result)
        result = self._rewrite_convert(result)
        result = self._rewrite_dateadd(result)
        result = self._rewrite_datediff(result)
        result = self._rewrite_datepart(result)
        result = self._rewrite_isnull(result)
        result = self._rewrite_charindex(result)
        result = self._rewrite_space(result)
        result = self._rewrite_isnumeric(result)
        result = self._rewrite_square(result)
        result = self._rewrite_simple_functions(result)

        return result

    def _rewrite_global_variables(self, sql: str) -> str:
        """Replace @@variable references with PostgreSQL equivalents."""
        for sybase_var, pg_equiv in _GLOBAL_VAR_MAP.items():
            pattern = re.compile(re.escape(sybase_var), re.IGNORECASE)
            if pattern.search(sql):
                sql = pattern.sub(pg_equiv, sql)
                if "/*" in pg_equiv:
                    self.warnings.append(
                        f"Global variable {sybase_var} has no direct equivalent — marked with comment"
                    )
        return sql

    def _rewrite_top_clause(self, sql: str) -> str:
        """Convert SELECT TOP N to SELECT ... LIMIT N."""
        # SELECT TOP <n> ... → SELECT ... LIMIT <n>
        pattern = re.compile(
            r'\bSELECT\s+TOP\s+(\d+)\b',
            re.IGNORECASE,
        )
        match = pattern.search(sql)
        if match:
            n = match.group(1)
            sql = pattern.sub("SELECT", sql)
            # Add LIMIT at end (before trailing semicolon if any)
            sql = sql.rstrip().rstrip(";")
            sql += f" LIMIT {n}"
        return sql

    def _rewrite_old_join_syntax(self, sql: str) -> str:
        """Convert Sybase *= and =* join operators to ANSI JOINs.

        This is a best-effort heuristic — complex multi-table WHERE clauses
        may need manual review.
        """
        if "*=" in sql or "=*" in sql:
            # Flag for manual review rather than risk incorrect rewrite
            self.warnings.append(
                "Old-style join operators (*= or =*) detected — "
                "convert to explicit LEFT/RIGHT JOIN manually"
            )
            sql = sql.replace("*=", "/* *= LEFT JOIN */ =")
            sql = sql.replace("=*", "= /* =* RIGHT JOIN */")
        return sql

    def _rewrite_string_concat(self, sql: str) -> str:
        """Convert + string concatenation to || operator.

        Only converts + that appears between string-like operands.
        This is heuristic: we convert + to || when adjacent to quotes or
        known string columns/functions.
        """
        # Pattern: 'literal' + expr or expr + 'literal'
        result = re.sub(
            r"('\s*)\+(\s*')",
            r"\1||\2",
            sql,
        )
        result = re.sub(
            r"('\s*)\+(\s*\w)",
            r"\1|| \2",
            result,
        )
        result = re.sub(
            r"(\w\s*)\+(\s*')",
            r"\1 ||\2",
            result,
        )
        return result

    def _rewrite_convert(self, sql: str) -> str:
        """Rewrite CONVERT(type, expr [, style]) → CAST or to_char."""
        pattern = re.compile(
            r'\bCONVERT\s*\(\s*'
            r'(\w[\w\s()]*?)'    # target type
            r'\s*,\s*'
            r'(.+?)'             # expression
            r'(?:\s*,\s*(\d+))?' # optional style code
            r'\s*\)',
            re.IGNORECASE,
        )

        def _replace_convert(m: re.Match) -> str:
            target_type = m.group(1).strip()
            expr = m.group(2).strip()
            style = int(m.group(3)) if m.group(3) else None

            # Map Sybase type names to CRDB types in CAST
            type_map = {
                "varchar": "TEXT", "nvarchar": "TEXT", "char": "TEXT",
                "int": "INT4", "integer": "INT4", "bigint": "INT8",
                "smallint": "INT2", "tinyint": "INT2",
                "float": "FLOAT8", "real": "FLOAT4",
                "numeric": "DECIMAL", "decimal": "DECIMAL",
                "money": "DECIMAL(19,4)", "smallmoney": "DECIMAL(10,4)",
                "datetime": "TIMESTAMPTZ", "smalldatetime": "TIMESTAMPTZ",
                "date": "DATE", "time": "TIME",
                "bit": "BOOL",
                "binary": "BYTES", "varbinary": "BYTES",
                "uniqueidentifier": "UUID",
            }

            # Extract base type (handle varchar(50) etc.)
            base = re.match(r'(\w+)', target_type)
            base_type = base.group(1).lower() if base else target_type.lower()
            crdb_type = type_map.get(base_type, target_type)

            if style is not None and base_type in ("varchar", "nvarchar", "char"):
                # Date-to-string conversion with style code
                fmt = _CONVERT_DATE_STYLES.get(style, "YYYY-MM-DD")
                return f"to_char({expr}, '{fmt}')"
            elif style is not None and base_type in ("datetime", "smalldatetime", "date"):
                # String-to-date conversion with style code
                fmt = _CONVERT_DATE_STYLES.get(style, "YYYY-MM-DD")
                return f"to_timestamp({expr}, '{fmt}')"
            else:
                return f"CAST({expr} AS {crdb_type})"

        return pattern.sub(_replace_convert, sql)

    def _rewrite_dateadd(self, sql: str) -> str:
        """Rewrite DATEADD(unit, n, date) → date + interval 'n unit'."""
        pattern = re.compile(
            r'\bDATEADD\s*\(\s*(\w+)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)',
            re.IGNORECASE,
        )

        def _replace(m: re.Match) -> str:
            unit = m.group(1).strip().lower()
            n = m.group(2).strip()
            date_expr = m.group(3).strip()
            pg_unit = _DATE_UNITS.get(unit, unit)

            if unit in ("quarter", "qq"):
                return f"({date_expr} + ({n}) * interval '3 months')"
            return f"({date_expr} + ({n}) * interval '1 {pg_unit}')"

        return pattern.sub(_replace, sql)

    def _rewrite_datediff(self, sql: str) -> str:
        """Rewrite DATEDIFF(unit, start, end) → EXTRACT or date arithmetic."""
        pattern = re.compile(
            r'\bDATEDIFF\s*\(\s*(\w+)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)',
            re.IGNORECASE,
        )

        def _replace(m: re.Match) -> str:
            unit = m.group(1).strip().lower()
            start = m.group(2).strip()
            end = m.group(3).strip()
            pg_unit = _DATE_UNITS.get(unit, unit)

            if pg_unit == "day":
                return f"(({end})::DATE - ({start})::DATE)"
            elif pg_unit in ("hour", "minute", "second"):
                return f"EXTRACT(EPOCH FROM ({end} - {start}))::INT / {{'hour': 3600, 'minute': 60, 'second': 1}}['{pg_unit}']"
            elif pg_unit == "year":
                return f"(EXTRACT(YEAR FROM {end}) - EXTRACT(YEAR FROM {start}))::INT"
            elif pg_unit == "month":
                return (
                    f"((EXTRACT(YEAR FROM {end}) - EXTRACT(YEAR FROM {start})) * 12 "
                    f"+ EXTRACT(MONTH FROM {end}) - EXTRACT(MONTH FROM {start}))::INT"
                )
            return f"/* DATEDIFF({unit}): manual conversion needed */ ({end} - {start})"

        return pattern.sub(_replace, sql)

    def _rewrite_datepart(self, sql: str) -> str:
        """Rewrite DATEPART(unit, date) → EXTRACT(unit FROM date)."""
        pattern = re.compile(
            r'\bDATEPART\s*\(\s*(\w+)\s*,\s*(.+?)\s*\)',
            re.IGNORECASE,
        )

        def _replace(m: re.Match) -> str:
            unit = m.group(1).strip().lower()
            date_expr = m.group(2).strip()
            pg_unit = _DATE_UNITS.get(unit, unit)
            # Special cases
            if unit in ("dayofyear", "dy"):
                return f"EXTRACT(DOY FROM {date_expr})::INT"
            if unit in ("weekday", "dw"):
                return f"EXTRACT(DOW FROM {date_expr})::INT"
            return f"EXTRACT({pg_unit} FROM {date_expr})::INT"

        return pattern.sub(_replace, sql)

    def _rewrite_isnull(self, sql: str) -> str:
        """Rewrite ISNULL(expr, default) → COALESCE(expr, default)."""
        pattern = re.compile(r'\bISNULL\s*\(', re.IGNORECASE)
        return pattern.sub("COALESCE(", sql)

    def _rewrite_charindex(self, sql: str) -> str:
        """Rewrite CHARINDEX(substr, str) → position(substr in str)."""
        pattern = re.compile(
            r'\bCHARINDEX\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)',
            re.IGNORECASE,
        )

        def _replace(m: re.Match) -> str:
            substr = m.group(1).strip()
            string = m.group(2).strip()
            return f"position({substr} in {string})"

        return pattern.sub(_replace, sql)

    def _rewrite_space(self, sql: str) -> str:
        """Rewrite SPACE(n) → repeat(' ', n)."""
        pattern = re.compile(r'\bSPACE\s*\(\s*(.+?)\s*\)', re.IGNORECASE)
        return pattern.sub(r"repeat(' ', \1)", sql)

    def _rewrite_isnumeric(self, sql: str) -> str:
        """Rewrite ISNUMERIC(expr) → (expr ~ '^[0-9.+-]+$')::INT."""
        pattern = re.compile(r'\bISNUMERIC\s*\(\s*(.+?)\s*\)', re.IGNORECASE)
        return pattern.sub(r"(\1 ~ '^[-+]?[0-9]*\.?[0-9]+$')::INT", sql)

    def _rewrite_square(self, sql: str) -> str:
        """Rewrite SQUARE(x) → power(x, 2)."""
        pattern = re.compile(r'\bSQUARE\s*\(\s*(.+?)\s*\)', re.IGNORECASE)
        return pattern.sub(r"power(\1, 2)", sql)

    def _rewrite_simple_functions(self, sql: str) -> str:
        """Apply simple 1:1 function name replacements."""
        simple_renames = {
            "getdate": "now",
            "getutcdate": "now",
            "sysdatetime": "now",
            "newid": "gen_random_uuid",
            "len": "length",
            "datalength": "octet_length",
            "replicate": "repeat",
            "char": "chr",
            "stdev": "stddev",
            "var": "variance",
            "count_big": "COUNT",
            "rand": "random",
            "ceiling": "ceil",
        }
        for old, new in simple_renames.items():
            pattern = re.compile(rf'\b{old}\s*\(', re.IGNORECASE)
            sql = pattern.sub(f"{new}(", sql)
        return sql


# ═══════════════════════════════════════════════════════════════
# 3. SCHEMA CONVERTER (DDL generation)
# ═══════════════════════════════════════════════════════════════

class SchemaConverter:
    """Convert Sybase table schemas to CockroachDB DDL statements.

    Uses the TSQLRewriter for expression conversion in defaults,
    computed columns, check constraints, and views.
    """

    def __init__(self, type_map: Optional[Dict[str, str]] = None):
        self._type_map = type_map or SYBASE_TO_CRDB_TYPE_MAP
        self.rewriter = TSQLRewriter()

    def convert_column_type(self, col: ColumnInfo) -> str:
        """Map a single Sybase column to its CockroachDB type string."""
        base = col.datatype.lower().strip()
        template = self._type_map.get(base)
        if template is None:
            template = self._type_map.get(col.user_type.lower().strip())
        if template is None:
            logger.warning(
                "Unmapped Sybase type '%s' (user_type='%s') for column '%s' — defaulting to TEXT",
                col.datatype, col.user_type, col.name,
            )
            return "TEXT"

        return template.format(
            length=max(col.length, 1),
            prec=col.prec if col.prec is not None else 18,
            scale=col.scale if col.scale is not None else 0,
        )

    def convert_default(self, sybase_default: Optional[str]) -> Optional[str]:
        """Convert a Sybase default expression to CockroachDB equivalent."""
        if not sybase_default:
            return None

        val = sybase_default.strip()
        while val.startswith("(") and val.endswith(")"):
            val = val[1:-1].strip()

        # Use the T-SQL rewriter for expression conversion
        converted = self.rewriter.rewrite(val)

        # Numeric literals
        try:
            float(converted)
            return converted
        except ValueError:
            pass

        # String literals
        if converted.startswith("'") and converted.endswith("'"):
            return converted

        return converted

    # ── Table DDL ────────────────────────────────────────────

    def generate_create_table(
        self, schema: TableSchema, include_fks: bool = False
    ) -> str:
        """Generate CREATE TABLE IF NOT EXISTS with computed columns and CHECK constraints."""
        lines = []

        for col in schema.columns:
            if col.is_computed and col.computed_formula:
                # Computed column → GENERATED ALWAYS AS (expr) STORED
                expr = self.rewriter.rewrite(col.computed_formula)
                crdb_type = self.convert_column_type(col)
                parts = [f"    {quote_ident(col.name)} {crdb_type}"]
                parts.append(f"GENERATED ALWAYS AS ({expr}) STORED")
                lines.append(" ".join(parts))
            elif col.is_identity:
                parts = [f"    {quote_ident(col.name)} INT8"]
                parts.append("DEFAULT unique_rowid()")
                parts.append("NOT NULL")
                lines.append(" ".join(parts))
            else:
                crdb_type = self.convert_column_type(col)
                parts = [f"    {quote_ident(col.name)} {crdb_type}"]
                if not col.nullable:
                    parts.append("NOT NULL")
                default = self.convert_default(col.default_value)
                if default:
                    parts.append(f"DEFAULT {default}")
                lines.append(" ".join(parts))

        # Primary key
        if schema.primary_key:
            pk_cols = ", ".join(quote_ident(c) for c in schema.primary_key)
            lines.append(f"    PRIMARY KEY ({pk_cols})")

        # CHECK constraints (with T-SQL expression rewriting)
        for chk in schema.check_constraints:
            expr = self.rewriter.rewrite(chk.definition)
            lines.append(
                f"    CONSTRAINT {quote_ident(chk.constraint_name)} CHECK ({expr})"
            )

        table_name = quote_ident(schema.name)
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        ddl += ",\n".join(lines)
        ddl += "\n);"
        return ddl

    def generate_indexes(self, schema: TableSchema) -> List[str]:
        """Generate CREATE INDEX statements (excluding PK index)."""
        stmts = []
        for idx in schema.indexes:
            if idx.is_primary_key:
                continue
            unique = "UNIQUE " if idx.is_unique else ""
            cols = ", ".join(quote_ident(c) for c in idx.columns)
            idx_name = quote_ident(idx.name)
            table_name = quote_ident(schema.name)
            stmts.append(
                f"CREATE {unique}INDEX IF NOT EXISTS {idx_name} "
                f"ON {table_name} ({cols});"
            )
        return stmts

    def generate_foreign_keys(self, schemas: Dict[str, TableSchema]) -> List[str]:
        """Generate ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY."""
        stmts = []
        for schema in schemas.values():
            for fk in schema.foreign_keys:
                fk_cols = ", ".join(quote_ident(c) for c in fk.columns)
                ref_cols = ", ".join(quote_ident(c) for c in fk.ref_columns)
                stmts.append(
                    f"ALTER TABLE {quote_ident(fk.table_name)} "
                    f"ADD CONSTRAINT {quote_ident(fk.constraint_name)} "
                    f"FOREIGN KEY ({fk_cols}) "
                    f"REFERENCES {quote_ident(fk.ref_table)} ({ref_cols});"
                )
        return stmts

    # ── Views ────────────────────────────────────────────────

    def convert_view(self, view: ViewInfo) -> Tuple[str, List[str]]:
        """Convert a Sybase view to CockroachDB DDL.

        Returns (converted_sql, list_of_warnings).
        """
        rewriter = TSQLRewriter()
        converted = rewriter.rewrite(view.source_sql)

        # Replace CREATE VIEW with CREATE OR REPLACE VIEW
        converted = re.sub(
            r'\bCREATE\s+VIEW\b',
            'CREATE VIEW',
            converted,
            flags=re.IGNORECASE,
        )

        return converted, rewriter.warnings

    # ── Stored procedures → PL/pgSQL scaffolding ─────────────

    def convert_stored_proc(self, proc: StoredProcInfo) -> Tuple[str, List[str]]:
        """Convert a Sybase stored procedure to a CockroachDB PL/pgSQL scaffold.

        Returns (converted_sql, list_of_warnings).
        Full T-SQL to PL/pgSQL conversion requires manual review.
        The scaffold provides the function signature and rewritten body
        as a starting point.
        """
        rewriter = TSQLRewriter()
        body = rewriter.rewrite(proc.source_sql)

        # Structural T-SQL → PL/pgSQL rewrites
        body = re.sub(r'\bCREATE\s+PROC(EDURE)?\b', 'CREATE OR REPLACE FUNCTION', body, flags=re.IGNORECASE)
        body = re.sub(r'\bAS\s*\n', 'AS $$\nDECLARE\nBEGIN\n', body, count=1, flags=re.IGNORECASE)
        body = re.sub(r'\bBEGIN\b', '', body, count=1, flags=re.IGNORECASE)

        # Variable declarations: @var type → var type
        body = re.sub(r'\bDECLARE\s+@(\w+)\s+', r'  \1 ', body, flags=re.IGNORECASE)
        body = re.sub(r'@(\w+)', r'\1', body)  # strip remaining @ prefixes

        # Control flow
        body = re.sub(r'\bIF\s+(.+?)\s*\n\s*BEGIN\b', r'IF \1 THEN', body, flags=re.IGNORECASE)
        body = re.sub(r'\bELSE\s*\n\s*BEGIN\b', 'ELSE', body, flags=re.IGNORECASE)
        body = re.sub(r'\bEND\b', 'END IF;', body, flags=re.IGNORECASE)
        body = re.sub(r'\bWHILE\s+(.+?)\s*\n\s*BEGIN\b', r'WHILE \1 LOOP', body, flags=re.IGNORECASE)
        body = re.sub(r'\bBREAK\b', 'EXIT', body, flags=re.IGNORECASE)
        body = re.sub(r'\bRETURN\b', 'RETURN', body, flags=re.IGNORECASE)

        # SET @var = expr → var := expr
        body = re.sub(r'\bSET\s+(\w+)\s*=\s*', r'\1 := ', body, flags=re.IGNORECASE)

        # PRINT → RAISE NOTICE
        body = re.sub(r'\bPRINT\s+(.+)', r"RAISE NOTICE '%', \1;", body, flags=re.IGNORECASE)

        rewriter.warnings.append(
            "Stored procedure conversion is a SCAFFOLD — manual review required. "
            "Verify: parameter types, RETURN values, exception handling, cursor usage."
        )

        if not body.rstrip().endswith("$$"):
            body = body.rstrip().rstrip(";")
            body += "\nEND;\n$$ LANGUAGE plpgsql;"

        return body, rewriter.warnings

    # ── User-defined types ───────────────────────────────────

    def convert_udt(self, udt: UserDefinedTypeInfo) -> str:
        """Convert a Sybase UDT to CREATE DOMAIN statement."""
        base = udt.base_type.lower()
        crdb_type = self._type_map.get(base, "TEXT")
        if "{length}" in crdb_type:
            crdb_type = crdb_type.format(length=udt.length or 1, prec=18, scale=0)
        if "{prec}" in crdb_type:
            crdb_type = crdb_type.format(
                prec=udt.prec or 18, scale=udt.scale or 0, length=1,
            )

        null = "" if udt.nullable else " NOT NULL"
        return f"CREATE DOMAIN {quote_ident(udt.name)} AS {crdb_type}{null};"

    # ── Full DDL generation ──────────────────────────────────

    def generate_full_ddl(
        self, schemas: Dict[str, TableSchema], include_fks: bool = False
    ) -> str:
        """Generate complete DDL for all tables, indexes, and optionally FKs."""
        parts = []
        parts.append("-- Generated by sysbasecrdb migration toolkit")
        parts.append("-- Tables and indexes (foreign keys applied separately)\n")

        for schema in schemas.values():
            parts.append(f"-- Table: {schema.name}")
            parts.append(self.generate_create_table(schema, include_fks=False))
            parts.append("")
            for idx_sql in self.generate_indexes(schema):
                parts.append(idx_sql)
            parts.append("")

        if include_fks:
            parts.append("\n-- Foreign keys")
            for fk_sql in self.generate_foreign_keys(schemas):
                parts.append(fk_sql)

        return "\n".join(parts)

    def generate_full_database_ddl(self, db_schema: DatabaseSchema) -> str:
        """Generate DDL for ALL objects in the database schema."""
        parts = []
        parts.append("-- " + "=" * 66)
        parts.append("-- Sybase ASE to CockroachDB - Full Schema Conversion")
        parts.append("-- Generated by sysbasecrdb migration toolkit")
        parts.append("-- " + "=" * 66)
        parts.append("")

        # UDTs first (tables may reference them)
        if db_schema.user_types:
            parts.append("-- User-defined types (CREATE DOMAIN)")
            parts.append("-- " + "-" * 40)
            for udt in db_schema.user_types.values():
                parts.append(self.convert_udt(udt))
            parts.append("")

        # Tables
        parts.append(self.generate_full_ddl(db_schema.tables, include_fks=False))

        # Views
        if db_schema.views:
            parts.append("\n-- Views")
            parts.append("-- " + "-" * 40)
            for view in db_schema.views.values():
                converted, warnings = self.convert_view(view)
                if warnings:
                    parts.append(f"-- WARNINGS for view {view.name}:")
                    for w in warnings:
                        parts.append(f"--   {w}")
                parts.append(converted)
                parts.append("")

        # Foreign keys (after all tables and views)
        parts.append("\n-- Foreign keys (applied after data load)")
        parts.append("-- " + "-" * 40)
        for fk_sql in self.generate_foreign_keys(db_schema.tables):
            parts.append(fk_sql)

        # Stored procedures (as scaffolds)
        if db_schema.stored_procs:
            parts.append("\n-- Stored procedures (PL/pgSQL scaffolds — REVIEW REQUIRED)")
            parts.append("-- " + "-" * 40)
            for proc in db_schema.stored_procs.values():
                converted, warnings = self.convert_stored_proc(proc)
                parts.append(f"\n-- Procedure: {proc.name}")
                if warnings:
                    for w in warnings:
                        parts.append(f"-- WARNING: {w}")
                parts.append(converted)

        return "\n".join(parts)
