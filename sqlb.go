package sqlb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"
)

type Query struct {
	query *strings.Builder
	args  []any
}

func NewQuery(query string, args ...any) Query {
	var q Query
	q.Append(query, args...)
	return q
}

func (q *Query) Append(query string, args ...any) {
	if want, got := strings.Count(query, "?"), len(args); want != got {
		panic(fmt.Sprintf("want %d args, got %d", want, got))
	}

	if q.query == nil {
		q.query = &strings.Builder{}
	}

	if q.query.Len() > 0 && q.query.String()[q.query.Len()-1] != ' ' {
		q.query.WriteRune(' ')
	}

	q.query.WriteString(query)
	q.args = append(q.args, args...)
}

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

type Updatable interface {
	PrimaryKey() string
	Values() []sql.NamedArg
}

func UpdateSQL(item Updatable) SQLer {
	var set bool
	var b Query
	for _, v := range item.Values() {
		if v.Name == item.PrimaryKey() {
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

type Insertable interface {
	PrimaryKey() string
	Values() []sql.NamedArg
}

func InsertSQL[T Insertable](items ...T) SQLer {
	if len(items) == 0 {
		panic("InsertSQL called with zero arguments")
	}

	first := items[0]
	firstValues := first.Values()

	columns := make([]string, 0, len(firstValues))
	for _, v := range firstValues {
		if v.Name == first.PrimaryKey() {
			continue
		}
		columns = append(columns, v.Name)
	}

	rows := make([]string, 0, len(items))
	values := make([]any, 0, len(columns)*len(items))
	for _, item := range items {
		placeholders := make([]string, 0, len(columns))
		for _, v := range item.Values() {
			if v.Name == item.PrimaryKey() {
				continue
			}
			placeholders = append(placeholders, "?")
			values = append(values, v.Value)
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	return NewQuery(
		fmt.Sprintf("(%s) VALUES %s", strings.Join(columns, ", "), strings.Join(rows, ", ")),
		values...,
	)
}

func InSQL[T any](items ...T) SQLer {
	if len(items) == 0 {
		panic("InsertSQL called with zero arguments")
	}

	placeholders := make([]string, len(items))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	values := make([]any, 0, len(items))
	for _, v := range items {
		values = append(values, v)
	}

	return NewQuery(fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")), values...)
}

type Scannable interface {
	ScanFrom(rows *sql.Rows) error
}

type ScanDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func Scan[T any, pT interface {
	Scannable
	*T
}](ctx context.Context, db ScanDB, dest *[]T, query string, args ...any) error {
	for t, err := range Iter[T, pT](ctx, db, query, args...) {
		if err != nil {
			return err
		}
		*dest = append(*dest, t)
	}
	return nil
}

func ScanPtr[T any, pT interface {
	Scannable
	*T
}](ctx context.Context, db ScanDB, dest *[]*T, query string, args ...any) error {
	for t, err := range Iter[T, pT](ctx, db, query, args...) {
		if err != nil {
			return err
		}
		*dest = append(*dest, &t)
	}
	return nil
}

func Iter[T any, pT interface {
	Scannable
	*T
}](ctx context.Context, db ScanDB, query string, args ...any) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		query, args = NewQuery(query, args...).SQL()

		if f := logFunc; f != nil {
			defer f(ctx, "query", query)()
		}

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t T
			if err := pT(&t).ScanFrom(rows); err != nil {
				var zero T
				if !yield(zero, err) {
					break
				}
				continue
			}
			if !yield(t, nil) {
				break
			}
		}
	}
}

func ScanRow[pT Scannable](ctx context.Context, db ScanDB, dest pT, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	if f := logFunc; f != nil {
		defer f(ctx, "query row", query)()
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}
	if err := dest.ScanFrom(rows); err != nil {
		return err
	}
	return nil
}

type ExecDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func Exec(ctx context.Context, db ExecDB, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	if f := logFunc; f != nil {
		defer f(ctx, "exec", query)()
	}

	_, err := db.ExecContext(ctx, query, args...)
	return err
}

type SQLer interface {
	SQL() (string, []any)
}

type primatives []any

func (p primatives) ScanFrom(rows *sql.Rows) error {
	return rows.Scan(p...)
}

func Values(dests ...any) Scannable {
	return primatives(dests)
}

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
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("want []byte, got %T", value)
	}
	return json.Unmarshal(b, &j.Data)
}

func (j JSON[T]) Value() (driver.Value, error) {
	return json.Marshal(j.Data)
}

var (
	logFuncMu sync.Mutex
	logFunc   func(ctx context.Context, typ string, query string) func()
)

type LogFunc func(ctx context.Context, typ string, duration time.Duration, query string)

func SetLog(f LogFunc) {
	logFuncMu.Lock()
	defer logFuncMu.Unlock()

	logFunc = func(ctx context.Context, typ string, query string) func() {
		start := time.Now()
		return func() {
			f(ctx, typ, time.Since(start), query)
		}
	}
}

type PrepareDB interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type StmtCache struct {
	mu    sync.RWMutex
	cache map[string]*sql.Stmt
	db    PrepareDB
}

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
