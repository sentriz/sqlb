# sqlb - low power tools for `database/sql`

[![godoc](https://img.shields.io/badge/pkg.go.dev-doc-blue)](http://pkg.go.dev/go.senan.xyz/sqlb)

## Features

- No-assumptions query builder suitable for dynamic SQL and arbitrary subqueries
- Type-safe and SQL injection-safe query composition
- Scanning results to interfaces, no use of reflection or struct tags

## Installation

To install `sqlb`, use `go get`:

```sh
go get go.senan.xyz/sqlb
```

## Query builder

### Dynamic SQL

```go
var b sqlb.Query
b.Append("SELECT * FROM users")
b.Append("WHERE 1")
if filterAge {
    b.Append("AND age > ?", 18)
}
if withStatus {
    b.Append("AND status = ?", status)
}
b.Append("AND id IN (?)", sqlb.NewQuery("SELECT id FROM admins WHERE level > ?", 5))

query, args := b.SQL()
// query "SELECT * FROM users WHERE age > ? AND status = ? AND id IN (SELECT id FROM admins WHERE level > ?)"
// args  []any{18, "active", 5}
```

In this example, the query builder extends the `query`, `[]arg` pattern by expanding any arg which implements the `SQLer` interface. This allows composing SQL fragments together while still being safe from SQL injections.

### More on the SQLer interface

Another SQLer is UpdateSQL(). This generates SQL suitable for `UPDATE` queries. The type to update needs to implement the `Updatable` interface.

```go
type Task struct {
    ID   int
    Name string
    Age  int
}

func (t Task) PrimaryKey() string {
    return "id"
}

func (t Task) Values() []sql.NamedArg {
    return []sql.NamedArg{
        sql.Named("id", t.ID),
        sql.Named("name", t.Name),
        sql.Named("age", t.Age),
    }
}

task := Task{ID: 1, Name: "Updated Task", Age: 25}

q := sqlb.NewQuery("UPDATE tasks SET ? WHERE id = ? RETURNING *", sqlb.UpdateSQL(task), task.ID)
query, args := q.SQL()
// query "UPDATE tasks SET name = ?, age = ? WHERE id = ? RETURNING *"
// args  []any{"Updated Task", 25, 1}
```

## Scanning results

### Slice of struct

```go
type User struct {
    ID   int
    Name string
    Age  int
}

func (u *User) ScanFrom(rows *sql.Rows) error {
    return rows.Scan(&u.ID, &u.Name, &u.Age)
}

var users []*User
err := sqlb.Scan(ctx, db, &users, "SELECT * FROM users WHERE age > ?", 18)
```

Here the `User` type implements the `Scannable` interface. This is a lightweight alternative to reflecting on the on input type as is type-safe

### Struct

Much the same as slice of struct but for when there is only one row

```go
var user User
err := sqlb.ScanRow(ctx, db, &user, "SELECT * FROM users WHERE id = ?", 3)
```

### Primative types

```go
var name string
var age int
err := sqlb.ScanRow(ctx, db, sqlb.Values(&name, &age), "SELECT name, age FROM users WHERE id = ?", 3)
```

## Full examples

### Dynamic SQL, pagination, counting without pagination, scanning

```go
var where sqlb.Query
where.Append("1")
if age != 0 {
    where.Append("AND age > ?", age)
}
if status != "" {
    where.Append("AND status = ?", status)
}

var total int
err = sqlb.ScanRow(ctx, db, sqlb.Values(&total), "SELECT count(1) FROM users WHERE ?", where)

var users []User
err = sqlb.Scan(ctx, db, &users, "SELECT * FROM users WHERE ? LIMIT ? OFFSET ?", where, limit, offset)
// or 
var users []*User
err = sqlb.ScanPtr(ctx, db, &users, "SELECT * FROM users WHERE ? LIMIT ? OFFSET ?", where, limit, offset)

// SELECT count(1) FROM users WHERE 1 AND age > ? AND status = ? LIMIT ? OFFSET ?
// SELECT * FROM users WHERE 1 AND age > ? AND status = ? LIMIT ? OFFSET ?
```

## License

This project is licensed under the [MIT License](LICENSE).
