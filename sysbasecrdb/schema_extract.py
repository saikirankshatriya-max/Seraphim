"""Extract schema metadata from Sybase ASE 16.x system catalogs.

Extracts ALL database object types:
  - Tables (columns, types, defaults, identity, computed columns)
  - Indexes (clustered, non-clustered, unique, PK)
  - Constraints (PK, FK, CHECK, UNIQUE)
  - Views (with source SQL)
  - Stored procedures (with source SQL)
  - User-defined functions (with source SQL)
  - Triggers (with source SQL)
  - User-defined types (UDTs)
  - Rules (reusable validation objects)
  - Default objects (reusable default bindings)
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .connections import SybaseConnection

logger = logging.getLogger("sysbasecrdb.schema_extract")


# ── Data classes for all object types ────────────────────────


@dataclass
class ColumnInfo:
    name: str
    datatype: str           # Sybase base type name (varchar, int, etc.)
    user_type: str          # User-defined type name if different
    length: int
    prec: Optional[int]     # Numeric precision
    scale: Optional[int]    # Numeric scale
    nullable: bool
    is_identity: bool
    is_computed: bool = False
    computed_formula: Optional[str] = None
    default_value: Optional[str] = None
    colid: int = 0          # Column ordinal


@dataclass
class IndexInfo:
    name: str
    table_name: str
    columns: List[str]
    is_unique: bool
    is_clustered: bool
    is_primary_key: bool


@dataclass
class ForeignKeyInfo:
    constraint_name: str
    table_name: str
    columns: List[str]
    ref_table: str
    ref_columns: List[str]


@dataclass
class CheckConstraintInfo:
    constraint_name: str
    table_name: str
    definition: str  # The T-SQL check expression


@dataclass
class ViewInfo:
    name: str
    owner: str
    source_sql: str  # Original T-SQL source from syscomments


@dataclass
class StoredProcInfo:
    name: str
    owner: str
    source_sql: str  # Original T-SQL source from syscomments


@dataclass
class UserFunctionInfo:
    name: str
    owner: str
    func_type: str   # 'FN' (scalar), 'IF' (inline table), 'TF' (table)
    source_sql: str


@dataclass
class TriggerInfo:
    name: str
    table_name: str
    events: List[str]  # ['INSERT'], ['UPDATE'], ['DELETE'], or combinations
    source_sql: str


@dataclass
class UserDefinedTypeInfo:
    name: str
    base_type: str       # Underlying Sybase base type
    length: Optional[int]
    prec: Optional[int]
    scale: Optional[int]
    nullable: bool
    bound_default: Optional[str]   # Name of bound default object
    bound_rule: Optional[str]      # Name of bound rule object


@dataclass
class RuleInfo:
    name: str
    definition: str  # The rule expression


@dataclass
class DefaultObjectInfo:
    name: str
    definition: str  # The default expression


@dataclass
class TableSchema:
    name: str
    owner: str
    columns: List[ColumnInfo] = field(default_factory=list)
    primary_key: Optional[List[str]] = None
    indexes: List[IndexInfo] = field(default_factory=list)
    foreign_keys: List[ForeignKeyInfo] = field(default_factory=list)
    check_constraints: List[CheckConstraintInfo] = field(default_factory=list)
    row_count_estimate: int = 0


@dataclass
class DatabaseSchema:
    """Complete schema snapshot of the entire Sybase database."""
    tables: Dict[str, TableSchema] = field(default_factory=dict)
    views: Dict[str, ViewInfo] = field(default_factory=dict)
    stored_procs: Dict[str, StoredProcInfo] = field(default_factory=dict)
    functions: Dict[str, UserFunctionInfo] = field(default_factory=dict)
    triggers: Dict[str, TriggerInfo] = field(default_factory=dict)
    user_types: Dict[str, UserDefinedTypeInfo] = field(default_factory=dict)
    rules: Dict[str, RuleInfo] = field(default_factory=dict)
    defaults: Dict[str, DefaultObjectInfo] = field(default_factory=dict)


class SchemaExtractor:
    """Extract schema information from Sybase ASE system catalogs."""

    def __init__(self, sybase_conn: SybaseConnection, database: str):
        self._conn = sybase_conn
        self._database = database

    def get_table_list(self) -> List[dict]:
        """Return all user table names with their owners."""
        sql = """
            SELECT o.name AS table_name, u.name AS owner
            FROM sysobjects o
            JOIN sysusers u ON o.uid = u.uid
            WHERE o.type = 'U'
              AND o.name NOT LIKE '_cdc_log_%'
            ORDER BY o.name
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [{"name": row[0], "owner": row[1]} for row in cur.fetchall()]

    def get_columns(self, table_name: str) -> List[ColumnInfo]:
        """Extract column details for a table, including computed columns."""
        sql = """
            SELECT
                c.name          AS col_name,
                t.name          AS type_name,
                ut.name         AS user_type_name,
                c.length        AS col_length,
                c.prec          AS col_prec,
                c.scale         AS col_scale,
                CASE WHEN c.status & 8 = 8 THEN 1 ELSE 0 END AS nullable,
                CASE WHEN c.status & 128 = 128 THEN 1 ELSE 0 END AS is_identity,
                CASE WHEN c.status2 & 1 = 1 THEN 1 ELSE 0 END AS is_computed,
                com.text        AS default_text,
                c.colid         AS col_ordinal
            FROM syscolumns c
            JOIN systypes t   ON c.type = t.type AND t.usertype < 100
            JOIN systypes ut  ON c.usertype = ut.usertype
            LEFT JOIN syscomments com ON com.id = c.cdefault AND com.colid = 1
            WHERE c.id = OBJECT_ID(:table_name)
            ORDER BY c.colid
        """.replace(":table_name", f"'{table_name}'")

        columns = []
        with self._conn.cursor() as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                default_val = row[9]
                if default_val:
                    default_val = default_val.strip()
                    if default_val.upper().startswith("DEFAULT "):
                        default_val = default_val[8:].strip()

                is_computed = bool(row[8])
                computed_formula = None
                if is_computed:
                    computed_formula = self._get_computed_formula(table_name, row[0])

                columns.append(ColumnInfo(
                    name=row[0],
                    datatype=row[1],
                    user_type=row[2],
                    length=row[3] or 0,
                    prec=row[4],
                    scale=row[5],
                    nullable=bool(row[6]),
                    is_identity=bool(row[7]),
                    is_computed=is_computed,
                    computed_formula=computed_formula,
                    default_value=default_val,
                    colid=row[10],
                ))
        return columns

    def _get_computed_formula(self, table_name: str, col_name: str) -> Optional[str]:
        """Extract the expression for a computed column from syscomments."""
        sql = f"""
            SELECT com.text
            FROM syscolumns c
            JOIN syscomments com ON com.id = c.computedcol
            WHERE c.id = OBJECT_ID('{table_name}')
              AND c.name = '{col_name}'
            ORDER BY com.colid
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                parts = [row[0] for row in cur.fetchall()]
                return "".join(parts).strip() if parts else None
        except Exception:
            return None

    def get_indexes(self, table_name: str) -> List[IndexInfo]:
        """Extract index information for a table."""
        # Sybase supports up to 31 index columns; we query up to 16 (practical limit)
        col_selects = ", ".join(
            f"INDEX_COL(o.name, i.indid, {n})" for n in range(1, 17)
        )
        sql = f"""
            SELECT
                i.name AS index_name,
                i.status,
                {col_selects}
            FROM sysindexes i
            JOIN sysobjects o ON i.id = o.id
            WHERE o.name = '{table_name}'
              AND i.indid > 0
              AND i.indid < 255
            ORDER BY i.indid
        """
        indexes = []
        with self._conn.cursor() as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                idx_name = row[0]
                status = row[1]
                # Collect non-NULL column names
                cols = [row[i] for i in range(2, 18) if row[i] is not None]
                if not cols:
                    continue

                is_unique = bool(status & 2)
                is_clustered = bool(status & 16)
                is_pk = bool(status & 2048)

                indexes.append(IndexInfo(
                    name=idx_name,
                    table_name=table_name,
                    columns=cols,
                    is_unique=is_unique,
                    is_clustered=is_clustered,
                    is_primary_key=is_pk,
                ))
        return indexes

    def get_primary_key(self, indexes: List[IndexInfo]) -> Optional[List[str]]:
        """Extract primary key columns from the index list."""
        for idx in indexes:
            if idx.is_primary_key:
                return idx.columns
        return None

    def get_foreign_keys(self, table_name: str) -> List[ForeignKeyInfo]:
        """Extract foreign key constraints where this table is the referencing table."""
        # Build column selects for up to 16 FK columns
        fk_selects = []
        for n in range(1, 17):
            fk_selects.append(f"COL_NAME(r.tableid, r.fokey{n})")
            fk_selects.append(f"COL_NAME(r.reftabid, r.refkey{n})")

        sql = f"""
            SELECT
                OBJECT_NAME(r.constrid) AS fk_name,
                OBJECT_NAME(r.tableid)  AS fk_table,
                OBJECT_NAME(r.reftabid) AS ref_table,
                {', '.join(fk_selects)}
            FROM sysreferences r
            WHERE r.tableid = OBJECT_ID('{table_name}')
        """
        fks = []
        with self._conn.cursor() as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                fk_name = row[0]
                fk_table = row[1]
                ref_table = row[2]

                # Parse paired fk/ref columns (indices 3 onward, in pairs)
                fk_cols = []
                ref_cols = []
                for i in range(0, 32, 2):
                    fk_col = row[3 + i]
                    ref_col = row[3 + i + 1]
                    if fk_col is not None and ref_col is not None:
                        fk_cols.append(fk_col)
                        ref_cols.append(ref_col)
                    else:
                        break

                if fk_cols:
                    fks.append(ForeignKeyInfo(
                        constraint_name=fk_name,
                        table_name=fk_table,
                        columns=fk_cols,
                        ref_table=ref_table,
                        ref_columns=ref_cols,
                    ))
        return fks

    def get_row_count_estimate(self, table_name: str) -> int:
        """Fast row count estimate via sysindexes (no full table scan)."""
        sql = f"""
            SELECT rowcnt(i.doampg)
            FROM sysindexes i
            WHERE i.id = OBJECT_ID('{table_name}')
              AND i.indid IN (0, 1)
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] else 0

    def get_check_constraints(self, table_name: str) -> List[CheckConstraintInfo]:
        """Extract CHECK constraints for a table."""
        sql = f"""
            SELECT
                OBJECT_NAME(c.constrid) AS constraint_name,
                com.text AS definition
            FROM sysconstraints c
            JOIN syscomments com ON com.id = c.constrid
            WHERE c.tableid = OBJECT_ID('{table_name}')
              AND c.status & 128 = 128
            ORDER BY c.constrid, com.colid
        """
        constraints: Dict[str, List[str]] = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    name = row[0]
                    text = row[1]
                    if name not in constraints:
                        constraints[name] = []
                    constraints[name].append(text)
        except Exception as exc:
            logger.debug("Could not extract check constraints for %s: %s", table_name, exc)
            return []

        return [
            CheckConstraintInfo(
                constraint_name=name,
                table_name=table_name,
                definition="".join(parts).strip(),
            )
            for name, parts in constraints.items()
        ]

    def get_table_schema(self, table_name: str, owner: str = "dbo") -> TableSchema:
        """Extract the full schema for a single table."""
        logger.info("Extracting schema for table: %s", table_name)

        columns = self.get_columns(table_name)
        indexes = self.get_indexes(table_name)
        primary_key = self.get_primary_key(indexes)
        foreign_keys = self.get_foreign_keys(table_name)
        check_constraints = self.get_check_constraints(table_name)
        row_count = self.get_row_count_estimate(table_name)

        return TableSchema(
            name=table_name,
            owner=owner,
            columns=columns,
            primary_key=primary_key,
            indexes=indexes,
            foreign_keys=foreign_keys,
            check_constraints=check_constraints,
            row_count_estimate=row_count,
        )

    def get_all_schemas(
        self, include: List[str], exclude: List[str]
    ) -> Dict[str, TableSchema]:
        """Extract schemas for all tables matching include/exclude patterns."""
        from .utils import filter_tables

        all_tables = self.get_table_list()
        table_names = [t["name"] for t in all_tables]
        filtered = filter_tables(table_names, include, exclude)

        owner_map = {t["name"]: t["owner"] for t in all_tables}

        logger.info(
            "Extracting schemas for %d tables (of %d total)",
            len(filtered),
            len(table_names),
        )

        schemas = {}
        for table_name in filtered:
            try:
                schemas[table_name] = self.get_table_schema(
                    table_name, owner=owner_map.get(table_name, "dbo")
                )
            except Exception as exc:
                logger.error("Failed to extract schema for %s: %s", table_name, exc)
                raise

        logger.info("Schema extraction complete: %d tables", len(schemas))
        return schemas

    # ── Non-table objects ────────────────────────────────────

    def _get_object_source(self, object_name: str) -> str:
        """Reassemble source SQL from syscomments (handles multi-row storage)."""
        sql = f"""
            SELECT com.text
            FROM syscomments com
            JOIN sysobjects o ON com.id = o.id
            WHERE o.name = '{object_name}'
            ORDER BY com.colid
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            parts = [row[0] for row in cur.fetchall()]
        return "".join(parts).strip() if parts else ""

    def get_views(self) -> Dict[str, ViewInfo]:
        """Extract all user views with their source SQL."""
        sql = """
            SELECT o.name, u.name AS owner
            FROM sysobjects o
            JOIN sysusers u ON o.uid = u.uid
            WHERE o.type = 'V'
            ORDER BY o.name
        """
        views = {}
        with self._conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        for row in rows:
            name, owner = row[0], row[1]
            source = self._get_object_source(name)
            if source:
                views[name] = ViewInfo(name=name, owner=owner, source_sql=source)
                logger.debug("Extracted view: %s", name)
        logger.info("Extracted %d views", len(views))
        return views

    def get_stored_procedures(self) -> Dict[str, StoredProcInfo]:
        """Extract all user stored procedures with their source SQL."""
        sql = """
            SELECT o.name, u.name AS owner
            FROM sysobjects o
            JOIN sysusers u ON o.uid = u.uid
            WHERE o.type = 'P'
              AND o.name NOT LIKE 'sp_%'
              AND o.name NOT LIKE '_cdc_trg_%'
            ORDER BY o.name
        """
        procs = {}
        with self._conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        for row in rows:
            name, owner = row[0], row[1]
            source = self._get_object_source(name)
            if source:
                procs[name] = StoredProcInfo(name=name, owner=owner, source_sql=source)
                logger.debug("Extracted stored procedure: %s", name)
        logger.info("Extracted %d stored procedures", len(procs))
        return procs

    def get_functions(self) -> Dict[str, UserFunctionInfo]:
        """Extract all user-defined functions with their source SQL."""
        # ASE 16 uses type 'FN' for scalar, 'IF' for inline table, 'TF' for table
        sql = """
            SELECT o.name, u.name AS owner, o.type
            FROM sysobjects o
            JOIN sysusers u ON o.uid = u.uid
            WHERE o.type IN ('FN', 'IF', 'TF')
            ORDER BY o.name
        """
        funcs = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

            for row in rows:
                name, owner, ftype = row[0], row[1], row[2].strip()
                source = self._get_object_source(name)
                if source:
                    funcs[name] = UserFunctionInfo(
                        name=name, owner=owner, func_type=ftype, source_sql=source,
                    )
        except Exception as exc:
            logger.debug("Could not extract functions (may not be supported): %s", exc)

        logger.info("Extracted %d user functions", len(funcs))
        return funcs

    def get_triggers(self) -> Dict[str, TriggerInfo]:
        """Extract all user triggers (excluding CDC triggers)."""
        sql = """
            SELECT o.name, OBJECT_NAME(o.deltrig) AS del_table,
                   OBJECT_NAME(o.instrig) AS ins_table,
                   OBJECT_NAME(o.updtrig) AS upd_table
            FROM sysobjects o
            WHERE o.type = 'TR'
              AND o.name NOT LIKE '_cdc_trg_%'
            ORDER BY o.name
        """
        # Alternative approach: query sysobjects for triggers directly
        sql2 = """
            SELECT tr.name AS trigger_name,
                   tab.name AS table_name
            FROM sysobjects tr
            JOIN sysobjects tab ON (
                tab.deltrig = tr.id OR tab.instrig = tr.id OR tab.updtrig = tr.id
            )
            WHERE tr.type = 'TR'
              AND tr.name NOT LIKE '_cdc_trg_%'
              AND tab.type = 'U'
            ORDER BY tr.name
        """
        triggers = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql2)
                rows = cur.fetchall()

            for row in rows:
                trg_name, table_name = row[0], row[1]
                source = self._get_object_source(trg_name)
                # Parse events from source (FOR INSERT, FOR UPDATE, etc.)
                events = []
                source_upper = source.upper()
                if "FOR INSERT" in source_upper or "AFTER INSERT" in source_upper:
                    events.append("INSERT")
                if "FOR UPDATE" in source_upper or "AFTER UPDATE" in source_upper:
                    events.append("UPDATE")
                if "FOR DELETE" in source_upper or "AFTER DELETE" in source_upper:
                    events.append("DELETE")

                if source:
                    triggers[trg_name] = TriggerInfo(
                        name=trg_name, table_name=table_name,
                        events=events, source_sql=source,
                    )
        except Exception as exc:
            logger.debug("Could not extract triggers: %s", exc)

        logger.info("Extracted %d user triggers", len(triggers))
        return triggers

    def get_user_defined_types(self) -> Dict[str, UserDefinedTypeInfo]:
        """Extract user-defined types from systypes."""
        sql = """
            SELECT
                ut.name             AS udt_name,
                bt.name             AS base_type,
                ut.length,
                ut.prec,
                ut.scale,
                CASE WHEN ut.allownulls = 1 THEN 1 ELSE 0 END AS nullable,
                OBJECT_NAME(ut.tdefault) AS bound_default,
                OBJECT_NAME(ut.domain)   AS bound_rule
            FROM systypes ut
            JOIN systypes bt ON ut.type = bt.type AND bt.usertype < 100
            WHERE ut.usertype >= 100
            ORDER BY ut.name
        """
        udts = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    name = row[0]
                    udts[name] = UserDefinedTypeInfo(
                        name=name,
                        base_type=row[1],
                        length=row[2],
                        prec=row[3],
                        scale=row[4],
                        nullable=bool(row[5]),
                        bound_default=row[6],
                        bound_rule=row[7],
                    )
        except Exception as exc:
            logger.debug("Could not extract UDTs: %s", exc)

        logger.info("Extracted %d user-defined types", len(udts))
        return udts

    def get_rules(self) -> Dict[str, RuleInfo]:
        """Extract rule objects (reusable validation rules)."""
        sql = """
            SELECT o.name
            FROM sysobjects o
            WHERE o.type = 'R'
            ORDER BY o.name
        """
        rules = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            for row in rows:
                name = row[0]
                defn = self._get_object_source(name)
                if defn:
                    rules[name] = RuleInfo(name=name, definition=defn)
        except Exception as exc:
            logger.debug("Could not extract rules: %s", exc)

        logger.info("Extracted %d rules", len(rules))
        return rules

    def get_default_objects(self) -> Dict[str, DefaultObjectInfo]:
        """Extract default objects (reusable default bindings)."""
        sql = """
            SELECT o.name
            FROM sysobjects o
            WHERE o.type = 'D'
            ORDER BY o.name
        """
        defaults = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            for row in rows:
                name = row[0]
                defn = self._get_object_source(name)
                if defn:
                    defaults[name] = DefaultObjectInfo(name=name, definition=defn)
        except Exception as exc:
            logger.debug("Could not extract defaults: %s", exc)

        logger.info("Extracted %d default objects", len(defaults))
        return defaults

    def get_full_database_schema(
        self, include: List[str], exclude: List[str]
    ) -> DatabaseSchema:
        """Extract the COMPLETE database schema — all object types."""
        logger.info("Starting full database schema extraction...")

        tables = self.get_all_schemas(include, exclude)
        views = self.get_views()
        stored_procs = self.get_stored_procedures()
        functions = self.get_functions()
        triggers = self.get_triggers()
        user_types = self.get_user_defined_types()
        rules = self.get_rules()
        defaults = self.get_default_objects()

        db_schema = DatabaseSchema(
            tables=tables,
            views=views,
            stored_procs=stored_procs,
            functions=functions,
            triggers=triggers,
            user_types=user_types,
            rules=rules,
            defaults=defaults,
        )

        logger.info(
            "Full extraction complete: %d tables, %d views, %d procs, "
            "%d functions, %d triggers, %d UDTs, %d rules, %d defaults",
            len(tables), len(views), len(stored_procs), len(functions),
            len(triggers), len(user_types), len(rules), len(defaults),
        )
        return db_schema
