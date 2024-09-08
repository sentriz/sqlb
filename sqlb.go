package sqlb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
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

func SubQuery(query string, args ...any) Query {
	var q Query
	q.Append("("+query+")", args...)
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

func UpdateSQL(u Updatable) Query {
	var set bool
	var b Query
	for _, v := range u.Values() {
		if v.Name == u.PrimaryKey() {
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

func InsertSQL(i Insertable) Query {
	insValues := i.Values()

	columns := make([]string, 0, len(insValues))
	placeholders := make([]string, 0, len(insValues))
	values := make([]any, 0, len(insValues))

	for _, v := range insValues {
		if v.Name == i.PrimaryKey() {
			continue
		}
		columns = append(columns, v.Name)
		placeholders = append(placeholders, "?")
		values = append(values, v.Value)
	}

	return NewQuery(
		fmt.Sprintf("(%s) VALUES (%s)", strings.Join(columns, ", "), strings.Join(placeholders, ", ")),
		values...,
	)
}

type Scannable interface {
	ScanFrom(rows *sql.Rows) error
}

type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func Scan[T Scannable](ctx context.Context, db DB, dest *[]T, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	newT := initT[T]()
	for rows.Next() {
		t := newT()
		if err := t.ScanFrom(rows); err != nil {
			return err
		}
		*dest = append(*dest, t)
	}
	return nil
}

func ScanRow[T Scannable](ctx context.Context, db DB, dest T, query string, args ...any) error {
	query, args = NewQuery(query, args...).SQL()

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

type SQLer interface {
	SQL() (string, []any)
}

type Primatives []any

func (p Primatives) ScanFrom(rows *sql.Rows) error {
	return rows.Scan(p...)
}

func Primative(dests ...any) Scannable {
	return Primatives(dests)
}

// initT can initialise and return a pointer to Type even if T is *Type
func initT[T any]() func() T {
	rT := reflect.TypeFor[T]()
	if rT.Kind() != reflect.Pointer {
		return func() T {
			var t T
			return t
		}
	}
	rT = rT.Elem()
	return func() T {
		return reflect.New(rT).Interface().(T)
	}
}
