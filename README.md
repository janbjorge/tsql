# ToySQL

ToySQL is a lightweight, in-memory SQL engine built for fun and learning purposes. It supports basic SQL operations like `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on in-memory tables.

## Features

- **CRUD Operations**:
  - `SELECT`: Retrieve rows with optional `WHERE` and `ORDER BY` clauses.
  - `INSERT`: Add rows to a table.
  - `UPDATE`: Modify rows based on conditions.
  - `DELETE`: Remove rows matching conditions.
- **In-Memory Storage**: Data is stored in Python dictionaries and lists.
- **SQL Parsing**: Queries are parsed and executed dynamically using Python lambdas.

## Requirements

- Python 3.13+

## Quickstart

```python
from toysql import ToyDatabase

# Create a database
db = ToyDatabase()

# Create a table
db.create_table("users", ["id", "name", "age"])

# Insert data
db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);")
db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);")

# Query data
print(db.execute("SELECT * FROM users;"))
print(db.execute("SELECT name FROM users WHERE age > 25 ORDER BY name;"))

# Update data
db.execute("UPDATE users SET age=35 WHERE id=1;")

# Delete data
db.execute("DELETE FROM users WHERE age < 30;")
```

## Example Output

```python
[{'id': '1', 'name': 'Alice', 'age': '30'}, {'id': '2', 'name': 'Bob', 'age': '25'}]
[{'name': 'Alice'}]
[{'id': '1', 'name': 'Alice', 'age': '35'}]
```

## Limitations

- **No Persistent Storage**: Data is lost when the program exits.
- **Basic SQL Support**: Only simple `WHERE` conditions and single-table queries are supported.
- **Single-Threaded**: Designed for experimentation, not performance.

## License

This project is licensed under the MIT License. Feel free to use and modify it for your own purposes.