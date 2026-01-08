// Package sqlb provides lightweight, type-safe, and reflection-free helpers for [database/sql].
//
// The core idea is that types implement [Scannable] to define how they scan themselves from rows,
// avoiding runtime reflection. Use sqlbgen to generate these implementations.
//
// # Query Functions
//
// These functions execute queries and scan results:
//
//   - [ScanRow]: scan first row into dest, returns [sql.ErrNoRows] if empty (scan straight into a [Scannable] type, or use [Values])
//   - [ScanRows]: scan all rows into dest (use with [Append], [AppendPtr], [AppendValue], [SetValue])
//   - [IterRows]: lazily iterate over rows
//   - [Exec]: execute without returning rows
//
// # Scannable Helpers
//
// These return [Scannable] implementations for use with [ScanRow] and [ScanRows] (though practically, for [ScanRow], only [Values] is useful):
//
//   - [Append]: append rows to *[]T (T must implement [Scannable])
//   - [AppendPtr]: append row pointers to *[]*T (T must implement [Scannable])
//   - [AppendValue]: append single column values to *[]T (for primitive types)
//   - [SetValue]: insert single column values into map[T]struct{} (for primitive types)
//   - [Values]: scan columns into pointers (for primitive types)
//
// # Code Generation
//
// Use sqlbgen to generate [Scannable], [Insertable], and [Updatable] implementations:
//
//	//go:generate go tool sqlbgen -to user.gen.go -generated ID User
//
// # Query Building
//
// [Query] provides composable query building with argument tracking:
//
//	var q sqlb.Query
//	q.Append("SELECT * FROM users WHERE 1")
//	if name != "" {
//	    q.Append("AND name = ?", name)
//	}
//
// Types implementing [SQLer] can be embedded as query arguments:
//
//	subquery := sqlb.NewQuery("SELECT id FROM admins WHERE level > ?", 5)
//	q.Append("AND id IN (?)", subquery)
//
// # Write Mapping
//
// [InsertSQL] and [UpdateSQL] generate SQL fragments for types implementing [Insertable] or [Updatable]:
//
//	sqlb.ScanRow(ctx, db, &user, "INSERT INTO users ? RETURNING *", sqlb.InsertSQL(user))
//	sqlb.ScanRow(ctx, db, &user, "UPDATE users SET ? WHERE id = ?", sqlb.UpdateSQL(user), user.ID)
//
// # Statement Caching
//
// [StmtCache] wraps a database connection to cache prepared statements:
//
//	cache := sqlb.NewStmtCache(db)
//	defer cache.Close()
//	sqlb.ScanRows(ctx, cache, sqlb.Append(&users), "SELECT * FROM users")
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

// Updatable represents a struct that can produce values for updates.
type Updatable interface {
	IsGenerated(column string) bool
	Values() []sql.NamedArg
}

// UpdateSQL builds a [SQLer] representing the update for the [Updatable].
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

// Insertable represents a struct that can provide values for insertion.
type Insertable interface {
	IsGenerated(column string) bool
	Values() []sql.NamedArg
}

// InsertSQL builds a [SQLer] representing an INSERT for one or more [Insertable] items.
// Generated columns (where [Insertable.IsGenerated] returns true) are skipped.
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

// Scannable represents a value that can be scanned from a row.
type Scannable interface {
	ScanFrom(columns []string, rows *sql.Rows, buf []any) error
}

// ScannablePtr is a constraint for pointer types that implement [Scannable].
// It allows sqlb to allocate a new T and scan its *T.
// It should not be used directly, since Go will infer it from the first type argument in [IterRows], [AppendPtr], and [Append].
type ScannablePtr[T any] interface {
	Scannable
	*T
}

// ScanDB is an interface compatible with [*sql.DB] or [*sql.Tx] for queries.
type ScanDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ScanRow executes the query and scans the first row into dest.
// Returns [sql.ErrNoRows] if no result is found.
func ScanRow(ctx context.Context, db ScanDB, dest Scannable, query string, args ...any) error {
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

// ScanRows executes the query and scans all rows into the single dest. To be used with [Append], [AppendPtr], [AppendValue], [SetValue], or [Values].
func ScanRows(ctx context.Context, db ScanDB, dest Scannable, query string, args ...any) error {
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

// IterRows returns a pull-based iterator ([iter.Seq2]) over query results.
// T must implement [Scannable] via its pointer type.
func IterRows[T any, pT ScannablePtr[T]](ctx context.Context, db ScanDB, query string, args ...any) iter.Seq2[T, error] {
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

// Append returns a [Scannable] that appends rows to dest.
// T must implement [Scannable].
func Append[T any, pT ScannablePtr[T]](dest *[]T) Scannable {
	return scanAppend[T, pT]{dest}
}

type scanAppend[T any, pT ScannablePtr[T]] struct {
	s *[]T
}

func (p scanAppend[T, pT]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var t T
	if err := pT(&t).ScanFrom(columns, rows, buf); err != nil {
		return err
	}
	*p.s = append(*p.s, t)
	return nil
}

// AppendPtr returns a [Scannable] that appends row pointers to dest.
// T must implement [Scannable].
func AppendPtr[T any, pT ScannablePtr[T]](dest *[]*T) Scannable {
	return scanAppendPtr[T, pT]{dest}
}

type scanAppendPtr[T any, pT ScannablePtr[T]] struct {
	s *[]*T
}

func (p scanAppendPtr[T, pT]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var t T
	if err := pT(&t).ScanFrom(columns, rows, buf); err != nil {
		return err
	}
	*p.s = append(*p.s, &t)
	return nil
}

// Values returns a [Scannable] that scans columns into the provided pointers.
// For use with primitive types.
func Values(dests ...any) Scannable {
	return scanValues(dests)
}

type scanValues []any

func (p scanValues) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	return rows.Scan(p...)
}

// AppendValue returns a [Scannable] that appends a single column value to dest.
// For use with primitive types.
func AppendValue[T any](s *[]T) Scannable {
	return (*scanAppendValue[T])(s)
}

type scanAppendValue[T any] []T

func (p *scanAppendValue[T]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var v T
	if err := rows.Scan(&v); err != nil {
		return err
	}
	*p = append(*p, v)
	return nil
}

// SetValue returns a [Scannable] that inserts a single column value into dest.
// For use with primitive types.
func SetValue[T comparable](s map[T]struct{}) Scannable {
	return (scanSet[T])(s)
}

type scanSet[T comparable] map[T]struct{}

func (p scanSet[T]) ScanFrom(columns []string, rows *sql.Rows, buf []any) error {
	var v T
	if err := rows.Scan(&v); err != nil {
		return err
	}
	p[v] = struct{}{}
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
	stmt, err := sc.getStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (sc *StmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := sc.getStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
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

	stmt, err := sc.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	sc.cache[query] = stmt
	return stmt, nil
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
		return fmt.Errorf("closing statements: %v", err)
	}
	return nil
}
