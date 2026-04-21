// Package sqlb provides lightweight, type-safe, and reflection-free helpers for [database/sql].
//
// sqlb tries to make few assumptions about how you write queries or read results.
// [Scanner] and [SQLer] interfaces can be implemented to help you extend the library.
// Built-in implementations are provided to cover common cases.
//
// # Reading rows
//
// Query functions execute queries and read results into a [Scanner]:
//
//   - [QueryRow]: read first row into dest
//   - [QueryRows]: read all rows into dest
//   - [Rows]: returns an iterator over all rows
//   - [Each]: returns an iterator with control over reading
//   - [Exec]: execute without returning rows
//
// A [Scanner] controls how rows are read. Built-in helpers cover common patterns:
//
//   - [Append]: append rows to *[]T
//   - [AppendPtr]: append row pointers to *[]*T
//   - [AppendValue]: for primitives, append single column values to *[]P
//   - [SetValue]: for primitives, insert single column values into map[P]struct{}
//   - [MapValues]: for primitives, insert two column values into map[K]V
//   - [Scan]: for primitives, read columns into pointers
//
// Or implement [Scanner] yourself for full control. [Scanner] implementations can also be generated
// by the companion tool `sqlbgen`.
//
//	var task Task
//	sqlb.QueryRow(ctx, db, &task, "SELECT * FROM tasks WHERE name = ?", "alice")
//
//	var tasks []Task
//	sqlb.QueryRows(ctx, db, sqlb.Append(&tasks), "SELECT * FROM tasks ORDER BY name")
//
//	for task, err := range sqlb.Rows[Task](ctx, db, "SELECT * FROM tasks ORDER BY name") {
//	    // ...
//	}
//
// # Writing rows
//
// [InsertSQL] and [UpdateSQL] generate [SQLer] fragments for types implementing [Insertable] or [Updatable]:
//
//	sqlb.QueryRow(ctx, db, &user, "INSERT INTO users ? RETURNING *", sqlb.InsertSQL(user))
//	sqlb.QueryRow(ctx, db, &user, "UPDATE users SET ? WHERE id = ? RETURNING *", sqlb.UpdateSQL(user), user.ID)
//
// # Query building
//
// [Query] provides composable query building with argument tracking:
//
//	var q sqlb.Query
//	q.Append("SELECT * FROM users WHERE 1")
//	if name != "" {
//	    q.Append("AND name = ?", name)
//	}
//	q.Append("ORDER BY name")
//
// Any argument implementing [SQLer] is expanded in-place. Several are built-in:
//
//	subquery := sqlb.NewQuery("SELECT id FROM admins WHERE level > ?", 5)
//	q.Append("AND id IN (?)", subquery)                       // built-in SQLer
//	q.Append("AND role IN ?", sqlb.InSQL("editor", "admin"))  // built-in SQLer
//	q.Append("AND ?", myCustomSQLer(x))                       // bring-your-own SQLer
//
// # Code generation
//
// Use sqlbgen to generate [Scanner], [Insertable], and [Updatable] implementations:
//
//	//go:generate go tool sqlbgen type User generated ID -- user.gen.go
//
// # Statement caching
//
// [StmtCache] wraps a database connection to cache prepared statements:
//
//	cache := sqlb.NewStmtCache(db)
//	defer cache.Close()
//	sqlb.QueryRows(ctx, cache, sqlb.Append(&users), "SELECT * FROM users")
//
// # Logging
//
// Use [WithLogFunc] to add query logging via context:
//
//	ctx := sqlb.WithLogFunc(ctx, func(ctx context.Context, typ, query string, dur time.Duration) {
//	    slog.DebugContext(ctx, "query", "type", typ, "query", query, "dur", dur)
//	})
package sqlb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"
)

// Query represents a composable SQL query builder with arguments.
type Query struct {
	query    *strings.Builder
	args     []any
	lastByte byte
}

// NewQuery creates a new Query by appending the initial query string and arguments.
func NewQuery(query string, args ...any) Query {
	var q Query
	q.Append(query, args...)
	return q
}

// Append adds a SQL fragment and corresponding arguments to the Query.
// The number of arguments must match the number of '?' placeholders in the fragment.
func (q *Query) Append(query string, args ...any) {
	if want, got := strings.Count(query, "?"), len(args); want != got {
		panic(fmt.Sprintf("want %d args, got %d", want, got))
	}

	if q.query == nil {
		q.query = &strings.Builder{}
	}

	if q.query.Len() > 0 && q.lastByte != ' ' {
		q.query.WriteByte(' ')
	}

	q.query.WriteString(query)
	if len(query) > 0 {
		q.lastByte = query[len(query)-1]
	}
	q.args = append(q.args, args...)
}

// SQL returns the composed SQL string and flattened argument slice.
// If any argument implements [SQLer], they are expanded recursively in-place.
func (q Query) SQL() (string, []any) {
	// fast path
	var hasSQLer bool
	for _, a := range q.args {
		if _, ok := a.(SQLer); ok {
			hasSQLer = true
			break
		}
	}
	if !hasSQLer {
		return q.query.String(), q.args
	}

	var query strings.Builder
	var args []any

	var count int
	for _, c := range q.query.String() {
		if c != '?' {
			query.WriteRune(c)
			continue
		}

		switch arg := q.args[count].(type) {
		case SQLer:
			q, ar := arg.SQL()
			query.WriteString(q)
			args = append(args, ar...)
		default:
			query.WriteRune(c)
			args = append(args, arg)
		}

		count++
	}

	return query.String(), args
}

// SQLer is implemented by types that can be embedded as query arguments.
type SQLer interface {
	SQL() (string, []any)
}

// Updatable represents a type that can provide column values for updates.
type Updatable interface {
	IsGenerated(column string) bool
	Values() []sql.NamedArg
}

// UpdateSQL builds a [SQLer] representing an UPDATE for an [Updatable] item.
// Generated columns (where [Updatable.IsGenerated] returns true) are skipped.
func UpdateSQL(item Updatable) SQLer {
	var set bool
	var b Query
	for _, v := range item.Values() {
		if item.IsGenerated(v.Name) {
			continue
		}
		var p string
		if set {
			p = ", "
		}
		b.Append(p+v.Name+"=?", v.Value)
		set = true
	}
	return b
}

// Insertable represents a type that can provide column values for insertion.
type Insertable interface {
	IsGenerated(column string) bool
	Values() []sql.NamedArg
}

// InsertSQL builds a [SQLer] representing an INSERT for one or more [Insertable] items.
// Generated columns (where [Insertable.IsGenerated] returns true) are skipped.
// Panics if called with zero items.
func InsertSQL[T Insertable](items ...T) SQLer {
	if len(items) == 0 {
		panic("InsertSQL called with zero arguments")
	}

	first := items[0]
	firstValues := first.Values()

	columns := make([]string, 0, len(firstValues))
	for _, v := range firstValues {
		if first.IsGenerated(v.Name) {
			continue
		}
		columns = append(columns, v.Name)
	}

	placeholders := slices.Repeat([]string{"?"}, len(columns))
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	rows := make([]string, len(items))
	values := make([]any, 0, len(columns)*len(items))
	for i, item := range items {
		for _, v := range item.Values() {
			if item.IsGenerated(v.Name) {
				continue
			}
			values = append(values, v.Value)
		}
		rows[i] = rowPlaceholder
	}

	return NewQuery(
		fmt.Sprintf("(%s) VALUES %s", strings.Join(columns, ", "), strings.Join(rows, ", ")),
		values...,
	)
}

// InSQL builds a [SQLer] for an IN clause or value tuple, e.g. (?, ?, ?).
// Panics if called with zero items.
func InSQL[T any](items ...T) SQLer {
	if len(items) == 0 {
		panic("InSQL called with zero arguments")
	}

	placeholders := slices.Repeat([]string{"?"}, len(items))
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	values := make([]any, 0, len(items))
	for _, v := range items {
		values = append(values, v)
	}

	return NewQuery(rowPlaceholder, values...)
}

// Scanner represents a type that can read itself from a row.
type Scanner interface {
	ScanFrom(columns []string, rows *sql.Rows, buf []any) error
}

// ScannerPtr is a constraint for pointer types that implement [Scanner], allowing allocation of a new T and read into *T.
// NOTE: It should not be used directly, since Go will infer it from destination arguments.
type ScannerPtr[T any] interface {
	Scanner
	*T
}

// QueryDB is an interface compatible with [*sql.DB] or [*sql.Tx] for querying rows.
type QueryDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// QueryRow executes the query and reads the first row into dest, which is typically
// a native [Scanner] type or one created with a [Scanner] helper such as [Scan].
// Returns [sql.ErrNoRows] if no rows are found.
func QueryRow(ctx context.Context, db QueryDB, dest Scanner, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	if lf := logFunc(ctx); lf != nil {
		defer log(ctx, lf, "query", query)()
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	buf := make([]any, 0, len(columns))
	if err := dest.ScanFrom(columns, rows, buf); err != nil {
		return err
	}
	return nil
}

// QueryRows executes the query and reads all rows into dest, which is typically
// a native [Scanner] type or one created with a [Scanner] helper like [Append].
func QueryRows(ctx context.Context, db QueryDB, dest Scanner, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	if lf := logFunc(ctx); lf != nil {
		defer log(ctx, lf, "query", query)()
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	buf := make([]any, 0, len(columns))
	for rows.Next() {
		if err := dest.ScanFrom(columns, rows, buf[:0]); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Rows returns an iterator over query results, allocating a new T per row.
// T must implement [Scanner] via its pointer type.
func Rows[T any, pT ScannerPtr[T]](ctx context.Context, db QueryDB, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		query, args = NewQuery(query, args...).SQL()

		if lf := logFunc(ctx); lf != nil {
			defer log(ctx, lf, "query", query)()
		}

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}

		buf := make([]any, 0, len(columns))
		for rows.Next() {
			var t T
			if err := pT(&t).ScanFrom(columns, rows, buf[:0]); err != nil {
				var zero T
				if !yield(zero, err) {
					return
				}
				continue
			}
			if !yield(t, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var zero T
			yield(zero, err)
			return
		}
	}
}

// Each returns an iterator that reads each row into dest.
// Unlike [Rows], it reuses the same dest each iteration, suitable for use with [Scanner] helpers like [Scan].
func Each(ctx context.Context, db QueryDB, dest Scanner, query string, args ...any) iter.Seq[error] {
	return func(yield func(error) bool) {
		query, args = NewQuery(query, args...).SQL()

		if lf := logFunc(ctx); lf != nil {
			defer log(ctx, lf, "query", query)()
		}

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(err)
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			yield(err)
			return
		}

		buf := make([]any, 0, len(columns))
		for rows.Next() {
			if err := dest.ScanFrom(columns, rows, buf[:0]); err != nil {
				if !yield(err) {
					return
				}
				continue
			}
			if !yield(nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(err)
			return
		}
	}
}

// ExecDB is an interface compatible with [*sql.DB] or [*sql.Tx] for executing queries.
type ExecDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Exec executes a query without returning any rows.
func Exec(ctx context.Context, db ExecDB, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	if lf := logFunc(ctx); lf != nil {
		defer log(ctx, lf, "exec", query)()
	}

	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// Append returns a [Scanner] that appends each row to dest.
func Append[T any, pT ScannerPtr[T]](dest *[]T) Scanner {
	return scannerAppend[T, pT]{dest}
}

type scannerAppend[T any, pT ScannerPtr[T]] struct {
	s *[]T
}

func (p scannerAppend[T, pT]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var t T
	if err := pT(&t).ScanFrom(columns, rows, buf); err != nil {
		return err
	}
	*p.s = append(*p.s, t)
	return nil
}

// AppendPtr returns a [Scanner] that appends a pointer to each row to dest.
func AppendPtr[T any, pT ScannerPtr[T]](dest *[]*T) Scanner {
	return scannerAppendPtr[T, pT]{dest}
}

type scannerAppendPtr[T any, pT ScannerPtr[T]] struct {
	s *[]*T
}

func (p scannerAppendPtr[T, pT]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var t T
	if err := pT(&t).ScanFrom(columns, rows, buf); err != nil {
		return err
	}
	*p.s = append(*p.s, &t)
	return nil
}

// Scan returns a [Scanner] that reads columns into the provided pointers.
// For primitive types that don't need a full [Scanner] implementation.
func Scan(dests ...any) Scanner {
	return scannerScan(dests)
}

type scannerScan []any

func (p scannerScan) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	return rows.Scan(p...)
}

// AppendValue returns a [Scanner] that appends a single column value to dest per row.
// For primitive types that don't need a full [Scanner] implementation.
func AppendValue[T any](s *[]T) Scanner {
	return (*scannerAppendValue[T])(s)
}

type scannerAppendValue[T any] []T

func (p *scannerAppendValue[T]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var v T
	if err := rows.Scan(&v); err != nil {
		return err
	}
	*p = append(*p, v)
	return nil
}

// SetValue returns a [Scanner] that inserts a single column value into dest per row.
// For primitive types that don't need a full [Scanner] implementation.
func SetValue[T comparable](s map[T]struct{}) Scanner {
	return (scannerSetValue[T])(s)
}

type scannerSetValue[T comparable] map[T]struct{}

func (p scannerSetValue[T]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var v T
	if err := rows.Scan(&v); err != nil {
		return err
	}
	p[v] = struct{}{}
	return nil
}

// MapValues returns a [Scanner] that reads two columns into dest as key-value pairs per row.
// For primitive types that don't need a full [Scanner] implementation.
func MapValues[K comparable, V any](m map[K]V) Scanner {
	return scannerMapValues[K, V](m)
}

type scannerMapValues[K comparable, V any] map[K]V

func (p scannerMapValues[K, V]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var k K
	var v V
	if err := rows.Scan(&k, &v); err != nil {
		return err
	}
	p[k] = v
	return nil
}

// JSON is a wrapper type for JSON-encoded database columns.
type JSON[T any] struct {
	Data T
}

var _ sql.Scanner = &JSON[struct{}]{}
var _ driver.Valuer = JSON[struct{}]{}

func NewJSON[T any](t T) JSON[T] {
	return JSON[T]{Data: t}
}

func (j *JSON[T]) Scan(value any) error {
	if value == nil {
		return nil
	}
	var b []byte
	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("want []byte or string, got %T", value)
	}
	return json.Unmarshal(b, &j.Data)
}

func (j JSON[T]) Value() (driver.Value, error) {
	return json.Marshal(j.Data)
}

// LogFunc is a callback for logging query execution.
type LogFunc = func(ctx context.Context, typ string, query string, dur time.Duration)

type logFuncContextKey struct{}

// WithLogFunc returns a context that will log queries using the provided function.
func WithLogFunc(ctx context.Context, lf LogFunc) context.Context {
	return context.WithValue(ctx, logFuncContextKey{}, lf)
}

func logFunc(ctx context.Context) LogFunc {
	f, _ := ctx.Value(logFuncContextKey{}).(LogFunc)
	return f
}

func log(ctx context.Context, lf LogFunc, typ string, query string) func() {
	start := time.Now()
	return func() {
		lf(ctx, typ, query, time.Since(start))
	}
}

// PrepareDB is an interface compatible with [*sql.DB] or [*sql.Tx] for preparing statements.
type PrepareDB interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// StmtCache wraps a database connection to cache prepared statements.
type StmtCache struct {
	mu    sync.RWMutex
	cache map[string]*sql.Stmt
	db    PrepareDB
}

// NewStmtCache creates a new statement cache wrapping the provided database connection.
func NewStmtCache(db PrepareDB) *StmtCache {
	return &StmtCache{
		cache: make(map[string]*sql.Stmt),
		db:    db,
	}
}

func (sc *StmtCache) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stmt, err := sc.getStmt(ctx, query) //nolint:sqlclosecheck // stmt is owned by cache, closed in Close
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (sc *StmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := sc.getStmt(ctx, query) //nolint:sqlclosecheck // stmt is owned by cache, closed in Close
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (sc *StmtCache) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	var errs []error
	for _, stmt := range sc.cache {
		if err := stmt.Close(); err != nil {
			errs = append(errs, err)
			continue
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("closing statements: %w", err)
	}
	return nil
}

func (sc *StmtCache) getStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	sc.mu.RLock()
	stmt, ok := sc.cache[query]
	sc.mu.RUnlock()
	if ok {
		return stmt, nil
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// check again in case another goroutine prepared it
	stmt, ok = sc.cache[query]
	if ok {
		return stmt, nil
	}

	stmt, err := sc.db.PrepareContext(ctx, query) //nolint:sqlclosecheck // stmt is stored in cache, closed in Close
	if err != nil {
		return nil, err
	}

	sc.cache[query] = stmt
	return stmt, nil
}
