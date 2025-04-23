# `sqlb`

Lightweight, type-safe, and reflection-free helpers for `database/sql`

[![godoc](https://img.shields.io/badge/pkg.go.dev-doc-blue)](http://pkg.go.dev/go.senan.xyz/sqlb)

## Installation

```sh
go get go.senan.xyz/sqlb
go get -tool go.senan.xyz/sqlb/cmd/sqlbgen # For code gen
```

## Features

### Query Building

```go
// Basic query building
q := sqlb.NewQuery("SELECT * FROM users WHERE age > ?", 18)

// Dynamic conditions
var q sqlb.Query
q.Append("SELECT * FROM users WHERE 1")
if name != "" {
    q.Append("AND name = ?", name)
}

// Composable queries via SQLer interface
subquery := sqlb.NewQuery("SELECT id FROM admins WHERE level > ?", 5)
q.Append("AND id IN (?)", subquery)

// Get the final SQL and args
// Note: Scan/ScanPtr/ScanRow/Exec do this automatically
sql, args := q.SQL()
```

### Data Scanning

```go
// db implements ScanDB (*sql.DB or *sql.Tx for transactions)

// User implements Scannable interface with ScanFrom(rows *sql.Rows) error
var users []User
err := sqlb.Scan(ctx, db, &users, "SELECT * FROM users")

// Scan into slice of pointers, which also implement Scannable
var users []*User
err := sqlb.ScanPtr(ctx, db, &users, "SELECT * FROM users")

// Scan a single row
var user User
err := sqlb.ScanRow(ctx, db, &user, "SELECT * FROM users WHERE id = ?", 1)

// Scan primitive values
var name string
var age int
err := sqlb.ScanRow(ctx, db, sqlb.Values(&name, &age), "SELECT name, age FROM users WHERE id = ?", 1)

// Iterate over results (Go 1.22+)
for user, err := range sqlb.Iter[User](ctx, db, "SELECT * FROM users") {
    if err != nil {
        // handle error
        continue
    }
    // process user
}
```

### CRUD Operations

```go
// User implements Insertable interface with PrimaryKey() and Values() methods
user := User{Name: "Alice", Age: 30}
err := sqlb.ScanRow(ctx, db, &user, "INSERT INTO users ? RETURNING *", sqlb.InsertSQL(user))

// Insert multiple records
users := []User{{Name: "Bob"}, {Name: "Charlie"}}
err := sqlb.Exec(ctx, db, "INSERT INTO users ?", sqlb.InsertSQL(users...))

// User implements Updatable interface with PrimaryKey() and Values() methods
user.Age = 31
err := sqlb.ScanRow(ctx, db, &user, "UPDATE users SET ? WHERE id = ? RETURNING *",
                   sqlb.UpdateSQL(user), user.ID)

// Execute a query
err := sqlb.Exec(ctx, db, "DELETE FROM users WHERE id = ?", 1)
```

### JSON Support

```go
type UserPrefs struct {
    Theme string
    Notifications bool
}

type User struct {
    ID int
    Name string
    Preferences sqlb.JSON[UserPrefs]
}

// JSON fields are automatically marshaled/unmarshaled
user.Preferences.Data.Theme = "dark"
err := sqlb.ScanRow(ctx, db, &user, "UPDATE users SET ? WHERE id = ? RETURNING *",
                   sqlb.UpdateSQL(user), user.ID)
```

### Query Logging

```go
sqlb.SetLog(func(ctx context.Context, typ string, duration time.Duration, query string) {
    slog.DebugContext(ctx, "Query executed", "type", typ, "query", query, "took", duration) // Values not logged
})
```

### Code Generation

```go
// Using as installed Go tool (recommended)
//go:generate go tool sqlbgen <TypeName> ...

// Alternately, execute directly
//go:generate go run go.senan.xyz/sqlb/cmd/sqlbgen <TypeName> ...

// Generates PrimaryKey(), Values(), and ScanFrom() implementations
```

## License

MIT
