from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class ParsedSQL:
    operation: str


@dataclass
class SelectSQL(ParsedSQL):
    columns: list[str]
    table: str
    where: str | None = None
    where_lambda: Callable[[dict], bool] | None = None
    orderby: str | None = None


@dataclass
class InsertSQL(ParsedSQL):
    table: str
    columns: list[str]
    values: list[str]


@dataclass
class UpdateSQL(ParsedSQL):
    table: str
    assignments: dict[str, str]
    where: str | None = None
    where_lambda: Callable[[dict], bool] | None = None


@dataclass
class DeleteSQL(ParsedSQL):
    table: str
    where: str | None = None
    where_lambda: Callable[[dict], bool] | None = None


class ToySQLParser:
    def __init__(self):
        self.select_regex = re.compile(
            r"""
            ^\s*SELECT\s+(?P<columns>[\w\*,\s]+)\s+
            FROM\s+(?P<table>\w+)
            (?:\s+WHERE\s+(?P<where>.+?))?
            (?:\s+ORDER\s+BY\s+(?P<orderby>\w+))?
            \s*;?\s*$
        """,
            re.IGNORECASE | re.VERBOSE,
        )

        self.insert_regex = re.compile(
            r"""
            ^\s*INSERT\s+INTO\s+(?P<table>\w+)\s+
            \((?P<columns>[\w,\s]+)\)\s+
            VALUES\s+\((?P<values>.+?)\)
            \s*;?\s*$
        """,
            re.IGNORECASE | re.VERBOSE,
        )

        self.update_regex = re.compile(
            r"""
            ^\s*UPDATE\s+(?P<table>\w+)\s+
            SET\s+(?P<assignments>.+?)
            (?:\s+WHERE\s+(?P<where>.+?))?
            \s*;?\s*$
        """,
            re.IGNORECASE | re.VERBOSE,
        )

        self.delete_regex = re.compile(
            r"""
            ^\s*DELETE\s+FROM\s+(?P<table>\w+)
            (?:\s+WHERE\s+(?P<where>.+?))?
            \s*;?\s*$
        """,
            re.IGNORECASE | re.VERBOSE,
        )

    def parse(self, query: str) -> ParsedSQL:
        query = query.strip()
        if query.upper().startswith("SELECT"):
            match = self.select_regex.match(query)
            if match:
                where = match.group("where")
                return SelectSQL(
                    operation="SELECT",
                    columns=[col.strip() for col in match.group("columns").split(",")],
                    table=match.group("table"),
                    where=where,
                    where_lambda=self._parse_where(where) if where else None,
                    orderby=match.group("orderby"),
                )
        elif query.upper().startswith("INSERT"):
            match = self.insert_regex.match(query)
            if match:
                return InsertSQL(
                    operation="INSERT",
                    table=match.group("table"),
                    columns=[col.strip() for col in match.group("columns").split(",")],
                    values=[val.strip() for val in match.group("values").split(",")],
                )
        elif query.upper().startswith("UPDATE"):
            match = self.update_regex.match(query)
            if match:
                where = match.group("where")
                assignments = {
                    k.strip(): v.strip()
                    for k, v in (
                        assignment.split("=")
                        for assignment in match.group("assignments").split(",")
                    )
                }
                return UpdateSQL(
                    operation="UPDATE",
                    table=match.group("table"),
                    assignments=assignments,
                    where=where,
                    where_lambda=self._parse_where(where) if where else None,
                )
        elif query.upper().startswith("DELETE"):
            match = self.delete_regex.match(query)
            if match:
                where = match.group("where")
                return DeleteSQL(
                    operation="DELETE",
                    table=match.group("table"),
                    where=where,
                    where_lambda=self._parse_where(where) if where else None,
                )
        raise ValueError("Query did not match any known SQL operation")

    def _parse_where(self, condition: str) -> Callable[[dict], bool]:
        condition_regex = re.compile(
            r"""
            (?P<left>\w+)\s*
            (?P<operator>=|!=|<|<=|>|>=)\s*
            (?P<right>.+)
        """,
            re.VERBOSE,
        )

        match = condition_regex.match(condition)
        if not match:
            raise ValueError(f"Unsupported WHERE condition: {condition}")

        left = match.group("left")
        operator = match.group("operator")
        right = match.group("right").strip()

        if operator == "=":
            return lambda row: row[left] == right
        elif operator == "!=":
            return lambda row: row[left] != right
        elif operator == "<":
            return lambda row: row[left] < right
        elif operator == "<=":
            return lambda row: row[left] <= right
        elif operator == ">":
            return lambda row: row[left] > right
        elif operator == ">=":
            return lambda row: row[left] >= right
        else:
            raise ValueError(f"Unsupported operator in WHERE condition: {operator}")


class ToyDB:
    def __init__(self):
        self.tables = dict[str, list[dict]]()
        self.parser = ToySQLParser()

    def execute(self, query: str):
        parsed = self.parser.parse(query)
        if isinstance(parsed, SelectSQL):
            return self._execute_select(parsed)
        elif isinstance(parsed, InsertSQL):
            self._execute_insert(parsed)
        elif isinstance(parsed, UpdateSQL):
            self._execute_update(parsed)
        elif isinstance(parsed, DeleteSQL):
            self._execute_delete(parsed)
        else:
            raise ValueError("Unsupported query type")

    def _execute_select(self, query: SelectSQL) -> list[dict]:
        table = self.tables.get(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")

        result = table
        if query.where_lambda:
            result = list(filter(query.where_lambda, result))
        if query.orderby:
            result.sort(key=lambda row: row[query.orderby])
        if query.columns != ["*"]:
            result = [{col: row[col] for col in query.columns} for row in result]
        return result

    def _execute_insert(self, query: InsertSQL):
        table = self.tables.get(query.table)
        if table is None:
            raise ValueError(f"Table '{query.table}' does not exist")

        table.append(
            {col: val for col, val in zip(query.columns, query.values, strict=True)},
        )

    def _execute_update(self, query: UpdateSQL):
        table = self.tables.get(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")

        for row in table:
            if query.where_lambda and not query.where_lambda(row):
                continue
            for col, val in query.assignments.items():
                row[col] = val

    def _execute_delete(self, query: DeleteSQL):
        table = self.tables.get(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")

        self.tables[query.table] = [
            row for row in table if not (query.where_lambda and query.where_lambda(row))
        ]

    def create_table(self, name: str, columns: list[str]):
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists")
        self.tables[name] = []


if __name__ == "__main__":
    import icecream

    db = ToyDB()
    db.create_table("users", ["id", "name", "age"])

    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);")
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);")
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);")

    icecream.ic(db.execute("SELECT * FROM users;"))
    icecream.ic(db.execute("SELECT * FROM users WHERE age > 30 ORDER BY name;"))
    icecream.ic(db.execute("UPDATE users SET age=40 WHERE id=1;"))
    icecream.ic(db.execute("SELECT * FROM users;"))
    icecream.ic(db.execute("DELETE FROM users WHERE age < 30;"))
    icecream.ic(db.execute("SELECT * FROM users;"))
