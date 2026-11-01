"""SQL enforcement: the single gate between the LLM's SQL and the database.

Principle: the text the LLM wrote is NEVER executed. It is parsed, checked
against an allow-list, rewritten (row filters, masking, row cap) and then
re-serialised from the syntax tree; only that regenerated SQL goes to the DB.

Pipeline (each step fails closed):
  1. parse            exactly one statement, size cap
  2. constructs       SELECT / set-operations only; no DML/DDL, INTO, locks, recursion,
                      LATERAL, bind params, reg* casts, table-functions
  3. functions        allow-list (typed by node class, unknown ones by name)
  4. complexity       joins / depth / node-count / cross-join limits
  5. qualify          resolve every column against the ROLE'S schema view (hidden => unresolved)
  6. access check     every table source and column of every scope is in the view;
                      masked columns only as plain projections
  7. rewrite          mask expressions, row-filter subqueries, LIMIT
  8. re-parse         the regenerated SQL must again be exactly one SELECT
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope

from natquery.access.functions import NEVER_ALLOWED, SAFE_ANONYMOUS, SAFE_TYPED
from natquery.access.policy import Policy, ResolvedRole
from natquery.access.schema_view import SchemaView
from natquery.auth.models import Identity
from natquery.errors import PermissionDenied, QueryRejected

_FORBIDDEN = tuple(
    getattr(exp, n)
    for n in [
        "Insert",
        "Update",
        "Delete",
        "Drop",
        "Alter",
        "Create",
        "TruncateTable",
        "Grant",
        "Revoke",
        "Command",
        "Set",
        "Copy",
        "Merge",
        "Use",
        "Transaction",
        "Commit",
        "Rollback",
        "Into",
        "Lock",
        "Lateral",
        "Pragma",
        "Analyze",
        "Kill",
        "Describe",
        "Show",
        "Execute",
        "LoadData",
        "Declare",
        "Attach",
        "Detach",
        "Comment",
        "Refresh",
        "Cache",
        "Uncache",
        "Export",
    ]
    if hasattr(exp, n)
)
_ATTR_VALUE = re.compile(r"^[\w@.\- ]{1,128}$")
_PLACEHOLDER_COL = "__natquery_col__"
BLOCKED_MSG = "That request was blocked by security policy."
GENERIC_REJECT = "I could not turn that into a valid query for the data you have access to. Try rephrasing."


@dataclass(frozen=True)
class EnforcedQuery:
    sql: str  # regenerated, safe-to-run SQL
    role: str
    tables: tuple[str, ...]
    masked_columns: tuple[str, ...]
    row_filtered_tables: tuple[str, ...]
    max_rows: int
    statement_timeout_ms: int
    redact: tuple[str, ...]


def _fixable(internal: str, reason: str) -> QueryRejected:
    return QueryRejected(GENERIC_REJECT, internal=internal, reason=reason, fixable=True)


def _blocked(internal: str, reason: str) -> PermissionDenied:
    return PermissionDenied(BLOCKED_MSG, internal=internal, reason=reason)


class SQLEnforcer:
    def __init__(
        self,
        policy: Policy,
        dialect: str = "postgres",
        allowed_schema: str = "public",
        allowed_schemas: Sequence[str] | None = None,
    ):
        self.policy = policy
        self.dialect = dialect
        self.allowed_schema = allowed_schema
        self.allowed_schemas = frozenset(
            s.lower() for s in (allowed_schemas or (allowed_schema,))
        )

    # ================================================================== public
    def enforce(
        self, sql: str, identity: Identity, role: ResolvedRole, view: SchemaView
    ) -> EnforcedQuery:
        lim = role.limits
        if view.is_empty():
            raise _blocked(f"role '{role.name}' has an empty schema view", "empty_view")

        ast = self._parse(sql, lim)
        self._check_constructs(ast)
        self._check_functions(ast)
        self._check_function_arguments(ast)
        self._check_complexity(ast, lim)

        if view.multi:
            self._resolve_unqualified_tables(ast, view)
        qualified = self._qualify(ast, view)
        tables, masked_nodes = self._check_access(qualified, view)

        used_filters = self._collect_filter_targets(qualified, view)
        bound = {
            t: self._bind_filter(view.row_filters[t], identity)
            for t in {n for _, n, _ in used_filters}
        }
        self._apply_masks(masked_nodes, view)
        self._apply_row_filters(used_filters, bound)
        final_ast = self._apply_limit(qualified, int(lim["max_rows"]))

        out_sql = final_ast.sql(dialect=self.dialect, comments=False)
        self._recheck(out_sql)
        return EnforcedQuery(
            sql=out_sql,
            role=role.name,
            tables=tuple(sorted(tables)),
            masked_columns=tuple(sorted(f"{t}.{c}" for t, c, _ in masked_nodes)),
            row_filtered_tables=tuple(sorted(bound)),
            max_rows=int(lim["max_rows"]),
            statement_timeout_ms=int(lim["statement_timeout_ms"]),
            redact=role.redact,
        )

    # ================================================================== 1. parse
    def _parse(self, sql: str, lim: Mapping) -> exp.Expression:
        if not sql or not sql.strip():
            raise _fixable("empty SQL", "empty")
        if len(sql) > int(lim["max_sql_length"]):
            raise _fixable("SQL exceeds max length", "too_long")
        if "\x00" in sql:
            raise _blocked("NUL byte in SQL", "nul_byte")
        try:
            stmts = [s for s in sqlglot.parse(sql, read=self.dialect) if s is not None]
        except Exception as e:
            raise _fixable(f"parse error: {str(e)[:200]}", "parse_error") from e
        if len(stmts) != 1:
            raise _fixable("exactly one statement is required", "multiple_statements")
        ast = stmts[0]
        if isinstance(ast, (exp.Subquery, exp.Paren)):
            ast = ast.unnest()
        return ast

    # ============================================================ 2. constructs
    def _check_constructs(self, ast: exp.Expression) -> None:
        if not isinstance(ast, (exp.Select, exp.SetOperation)):
            raise _blocked(
                f"statement type {type(ast).__name__} is not allowed", "not_select"
            )
        for node in ast.walk():
            if isinstance(node, _FORBIDDEN):
                raise _blocked(
                    f"forbidden construct {type(node).__name__}", "forbidden_construct"
                )
            if isinstance(node, exp.Select):
                if node.args.get("into"):
                    raise _blocked("SELECT INTO is not allowed", "select_into")
                if node.args.get("locks"):
                    raise _blocked("locking clauses are not allowed", "row_lock")
            elif isinstance(node, (exp.Placeholder, exp.Parameter)):
                raise _fixable("bind parameters are not supported", "bind_param")
            elif isinstance(node, exp.With) and node.args.get("recursive"):
                raise _fixable("recursive queries are not supported", "recursive")
            elif isinstance(node, exp.Table):
                if not isinstance(node.this, exp.Identifier):
                    raise _fixable(
                        "table-valued functions are not supported", "table_function"
                    )
                if node.args.get("catalog") or (
                    node.db and node.db.lower() not in self.allowed_schemas
                ):
                    raise _blocked(
                        "references a schema outside the allowed one", "other_schema"
                    )
            elif isinstance(node, exp.DataType):
                text = node.sql(dialect=self.dialect).lower()
                if text.startswith("reg") or text == "oid":
                    raise _blocked(f"cast to catalog type {text}", "catalog_cast")

    # ============================================================== 3. functions
    def _check_functions(self, ast: exp.Expression) -> None:
        extra = self.policy.allowed_functions
        for node in ast.find_all(exp.Func):
            if isinstance(node, exp.Anonymous):
                key = (node.name or "").lower()
                if key in NEVER_ALLOWED:
                    raise _blocked(
                        f"function {key} is never allowed", "dangerous_function"
                    )
                ok = key in SAFE_ANONYMOUS or key in extra
            else:
                key = type(node).__name__.lower()
                ok = key in SAFE_TYPED or key in extra
            if not ok:
                raise _fixable(
                    f"function '{key}' is not allowed; use standard aggregate/scalar SQL functions",
                    "function_not_allowed",
                )

    # ================================================== 3b. function arguments
    _PAD_MAX = 10_000

    def _check_function_arguments(self, ast: exp.Expression) -> None:
        """Allowed functions can still be abused through their arguments (LPAD(x, 1000000000))."""
        for node in ast.find_all(exp.Pad):
            n = node.args.get("expression")
            if not (
                isinstance(n, exp.Literal)
                and not n.is_string
                and n.name.isdigit()
                and int(n.name) <= self._PAD_MAX
            ):
                raise _fixable(
                    f"LPAD/RPAD length must be a number up to {self._PAD_MAX}",
                    "function_argument",
                )

    # ============================================================ 4. complexity
    def _check_complexity(self, ast: exp.Expression, lim: Mapping) -> None:
        if sum(1 for _ in ast.walk()) > int(lim["max_ast_nodes"]):
            raise _fixable("query is too large", "too_complex")
        joins = list(ast.find_all(exp.Join))
        if len(joins) > int(lim["max_joins"]):
            raise _fixable(f"more than {lim['max_joins']} joins", "too_many_joins")
        if not lim.get("allow_cross_join"):
            for j in joins:
                on = j.args.get("on")
                has_cond = (
                    # ON TRUE / ON 1=1 is a cross join in disguise: the condition must mention a column
                    (on is not None and on.find(exp.Column) is not None)
                    or j.args.get("using")
                    or (j.args.get("method") or "") == "NATURAL"
                )
                if not has_cond:
                    raise _fixable(
                        "every join needs a join condition (no cross joins)",
                        "cross_join",
                    )
        depth = max(
            (self._select_depth(s) for s in ast.find_all(exp.Select)), default=0
        )
        if depth > int(lim["max_subquery_depth"]):
            raise _fixable("subqueries nested too deeply", "too_deep")

    @staticmethod
    def _select_depth(node: exp.Expression) -> int:
        depth, cur = 0, node.parent
        while cur is not None:
            if isinstance(cur, exp.Select):
                depth += 1
            cur = cur.parent
        return depth

    # ============================================= 4b. multi-schema table names
    @staticmethod
    def _tkey(t: exp.Table, multi: bool) -> str:
        return f"{t.db}.{t.name}" if multi else t.name

    @staticmethod
    def _resolve_unqualified_tables(ast: exp.Expression, view: SchemaView) -> None:
        """Multi-schema mode: add the schema to a bare table name when exactly one visible table has
        that name. Ambiguous names must be qualified by the caller."""
        ctes = {c.alias_or_name.lower() for c in ast.find_all(exp.CTE)}
        by_name: dict[str, list[str]] = {}
        for key in view.tables:
            ns, _, table = key.partition(".")
            by_name.setdefault(table.lower(), []).append(ns)
        for t in ast.find_all(exp.Table):
            if t.db or t.name.lower() in ctes:
                continue
            candidates = by_name.get(t.name.lower(), [])
            if len(candidates) == 1:
                t.set("db", exp.to_identifier(candidates[0]))
            elif len(candidates) > 1:
                raise _fixable(
                    f"table '{t.name}' exists in several schemas ({', '.join(sorted(candidates))}); "
                    "write schema.table",
                    "ambiguous_table",
                )

    # ================================================================ 5. qualify
    def _qualify(self, ast: exp.Expression, view: SchemaView) -> exp.Expression:
        try:
            return qualify(
                ast.copy(),
                schema=view.sqlglot_schema(),
                dialect=self.dialect,
                validate_qualify_columns=True,
                expand_stars=True,
                identify=False,
            )
        except Exception as e:
            raise _fixable(
                f"unresolved reference: {str(e)[:200]}", "unresolved_reference"
            ) from e

    # ============================================================ 6. access check
    def _check_access(self, q: exp.Expression, view: SchemaView):
        tables: set[str] = set()
        masked_nodes: list[tuple[str, str, exp.Column]] = []
        seen: set[int] = set()

        scopes = traverse_scope(q)
        for sc in scopes:
            for src in sc.sources.values():
                if isinstance(src, exp.Table):
                    key = self._tkey(src, view.multi)
                    if key not in view.tables:
                        raise _fixable(
                            f"unresolved reference: table '{key}' is not available",
                            "unresolved_reference",
                        )
                    tables.add(key)

        self._check_whole_row_references(q)

        for sc in scopes:
            external = {id(c) for c in sc.external_columns}
            outputs = set(getattr(sc.expression, "named_selects", []) or [])
            for col in sc.columns:
                if id(col) in seen:
                    continue
                if (
                    not col.table
                ):  # reference to an output alias (ORDER BY n) - must exist
                    if col.name not in outputs:
                        raise _fixable(
                            f"unresolved reference: column '{col.name}'",
                            "unresolved_reference",
                        )
                    seen.add(id(col))
                    continue
                src = sc.sources.get(col.table)
                if src is None:
                    if (
                        id(col) in external
                    ):  # owned by an enclosing scope, checked there
                        continue
                    raise _fixable(
                        f"unresolved reference: '{col.table}.{col.name}'",
                        "unresolved_reference",
                    )
                seen.add(id(col))
                if not isinstance(src, exp.Table):
                    continue  # derived table / CTE: its inner scope was checked on its own
                real = self._tkey(src, view.multi)
                if col.name not in view.tables[real]:
                    raise _fixable(
                        f"unresolved reference: column '{real}.{col.name}'",
                        "unresolved_reference",
                    )
                if col.name in view.masked.get(real, {}):
                    if not self._is_plain_projection(col):
                        raise _fixable(
                            f"column '{real}.{col.name}' is display-only: select it directly, "
                            "do not filter, sort, group, join or aggregate on it",
                            "masked_misuse",
                        )
                    masked_nodes.append((real, col.name, col))

        for star in q.find_all(exp.Star):
            if not isinstance(star.parent, exp.Func):
                raise _fixable("could not expand '*'", "star_unexpanded")
        return tables, masked_nodes

    @staticmethod
    def _check_whole_row_references(q: exp.Expression) -> None:
        """Refuse ``SELECT t FROM t``, ``t::text``, ``(t).col``: a bare table name or alias used as a
        value is the table's whole row, which would carry columns the role may not see.

        After qualification every legitimate identifier is a column name, a table or alias name, or an
        output alias. An identifier anywhere else is a whole-row reference."""
        for node in q.find_all(exp.Identifier):
            parent = node.parent
            if isinstance(parent, (exp.Column, exp.Table, exp.TableAlias)):
                continue
            if isinstance(parent, exp.Alias) and node.arg_key == "alias":
                continue
            raise _blocked(
                "whole-row reference (a table or alias used as a value) is not allowed",
                "whole_row_reference",
            )

    @staticmethod
    def _is_plain_projection(col: exp.Column) -> bool:
        parent = col.parent
        if isinstance(parent, exp.Alias) and parent.this is col:
            return (
                isinstance(parent.parent, exp.Select)
                and parent.arg_key == "expressions"
            )
        return isinstance(parent, exp.Select) and col.arg_key == "expressions"

    # ================================================================ 7. rewrite
    def _apply_masks(self, masked_nodes, view: SchemaView) -> None:
        for table, column, col in masked_nodes:
            template = self.policy.mask_template(view.masked[table][column])
            expr = sqlglot.parse_one(
                template.replace("{col}", _PLACEHOLDER_COL), read=self.dialect
            )
            for ph in list(expr.find_all(exp.Column)):
                if ph.name == _PLACEHOLDER_COL:
                    ph.replace(col.copy())
            parent = col.parent
            if isinstance(parent, exp.Alias) and parent.this is col:
                col.replace(expr)
            else:  # direct projection without alias: keep the output name
                col.replace(exp.alias_(expr, column, quoted=False))

    def _collect_filter_targets(self, q: exp.Expression, view: SchemaView):
        targets = []
        for sc in traverse_scope(q):
            for alias, src in sc.sources.items():
                key = (
                    self._tkey(src, view.multi) if isinstance(src, exp.Table) else None
                )
                if key is not None and key in view.row_filters:
                    targets.append((src, key, alias))
        return targets

    def _bind_filter(self, template: str, identity: Identity) -> exp.Expression:
        cond = sqlglot.condition(template, dialect=self.dialect)
        for ph in list(cond.find_all(exp.Placeholder)):
            value = identity.attributes.get(ph.name)
            if value is None:
                raise _blocked(
                    f"user '{identity.username}' lacks attribute '{ph.name}' needed by a row filter",
                    "missing_attribute",
                )
            value = str(value)
            if not _ATTR_VALUE.fullmatch(value):
                raise _blocked(
                    f"attribute '{ph.name}' has an unsafe value", "unsafe_attribute"
                )
            ph.replace(exp.Literal.string(value))
        return cond

    @staticmethod
    def _table_node(key: str) -> exp.Table:
        ns, dot, table = key.partition(".")
        if not dot:
            return exp.Table(this=exp.to_identifier(key))
        return exp.Table(this=exp.to_identifier(table), db=exp.to_identifier(ns))

    def _apply_row_filters(self, targets, bound: Mapping[str, exp.Expression]) -> None:
        for src, name, alias in targets:
            inner = (
                exp.select("*").from_(self._table_node(name)).where(bound[name].copy())
            )
            src.replace(
                exp.Subquery(
                    this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias))
                )
            )

    @staticmethod
    def _apply_limit(ast: exp.Expression, max_rows: int) -> exp.Expression:
        if isinstance(ast, exp.SetOperation):
            return exp.select("*").from_(ast.subquery("_nq_result")).limit(max_rows)
        lim = ast.args.get("limit")
        current = None
        if (
            isinstance(lim, exp.Limit)
            and isinstance(lim.expression, exp.Literal)
            and not lim.expression.is_string
        ):
            try:
                current = int(lim.expression.this)
            except ValueError:
                current = None
        if current is None or current > max_rows:
            ast.set("limit", exp.Limit(expression=exp.Literal.number(max_rows)))
        return ast

    # ================================================================ 8. re-check
    def _recheck(self, sql: str) -> None:
        try:
            stmts = [s for s in sqlglot.parse(sql, read=self.dialect) if s is not None]
        except Exception as e:  # pragma: no cover - defensive
            raise _blocked(f"regenerated SQL does not parse: {e}", "regen_parse") from e
        if len(stmts) != 1 or not isinstance(stmts[0], (exp.Select, exp.SetOperation)):
            raise _blocked("regenerated SQL is not a single SELECT", "regen_shape")
