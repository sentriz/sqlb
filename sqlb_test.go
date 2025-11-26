package sqlb_test

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"path/filepath"
	"testing"
	"time"

	"github.com/carlmjohnson/be"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"go.senan.xyz/sqlb"
)

func TestBuild(t *testing.T) {
	t.Parallel()

	var b sqlb.Query
	b.Append("select * from tasks")
	b.Append("where 1")
	b.Append("and one=?", 1)
	b.Append("and two=?", 2)
	b.Append("returning *")

	query, args := b.SQL()
	be.Equal(t, "select * from tasks where 1 and one=? and two=? returning *", query)
	be.DeepEqual(t, []any{1, 2}, args)
}

func TestBuildSubquery(t *testing.T) {
	t.Parallel()

	var whereA sqlb.Query
	whereA.Append("three=?", 3)

	whereB := sqlb.NewQuery("four=?", 4)

	var whereC sqlb.Query
	whereC.Append("? or ?", whereA, whereB)

	where := sqlb.NewQuery("?", whereC)

	var b sqlb.Query
	b.Append("select * from (?) union (?)",
		sqlb.NewQuery("select * from tasks where a=?",
			"a",
		),
		sqlb.NewQuery("select * from tasks where a=? and ?",
			"aa",
			sqlb.NewQuery("xx=?", 10),
		),
	)
	b.Append("where ?", where)

	query, args := b.SQL()
	be.Equal(t, "select * from (select * from tasks where a=?) union (select * from tasks where a=? and xx=?) where three=? or four=?", query)
	be.DeepEqual(t, []any{"a", "aa", 10, 3, 4}, args)
}

func TestBuildPanic(t *testing.T) {
	t.Parallel()

	expPanic := "want 3 args, got 2"

	defer func() {
		if r := recover(); r != expPanic {
			t.Errorf("exp panic %q got %q", expPanic, r)
		}
	}()

	var b sqlb.Query
	b.Append("one=?, two=?, three=?", 1, 2)
}

func TestInSQL(t *testing.T) {
	query, args := sqlb.InSQL("one", "two", "three").SQL()
	be.Equal(t, "(?, ?, ?)", query)
	be.DeepEqual(t, []any{"one", "two", "three"}, args)
}

func TestInsert(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	task := Task{Name: "the name", Age: 32}
	err := sqlb.ScanRow(ctx, db, &task, "insert into tasks ? returning *", sqlb.InsertSQL(task))
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 1, Name: "the name", Age: 32}, task)
}

func TestInsertUpdate(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	task := Task{Name: "name", Age: 100}

	err := sqlb.ScanRow(ctx, db, &task, "insert into tasks ? returning *", sqlb.InsertSQL(task))
	be.NilErr(t, err)
	be.Nonzero(t, task.ID)

	task.Age = 101
	err = sqlb.ScanRow(ctx, db, &task, "update tasks set ? returning *", sqlb.UpdateSQL(task))
	be.NilErr(t, err)

	var readTask Task
	err = sqlb.ScanRow(ctx, db, &readTask, "select * from tasks where id = ?", task.ID)
	be.NilErr(t, err)
	be.Equal(t, 101, readTask.Age)
	be.DeepEqual(t, task, readTask)
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := t.Context()

	var task Task
	err := sqlb.ScanRow(ctx, db, &task, "insert into tasks (name) values (?) returning *", "the name")
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 1, Name: "the name", Age: 0}, task)

	task.Age = 69

	build := sqlb.NewQuery("update tasks set ? where id=? returning *", sqlb.UpdateSQL(task), task.ID)
	query, args := build.SQL()

	be.Equal(t, "update tasks set name=? , age=? where id=? returning *", query)
	be.DeepEqual(t, []any{"the name", 69, 1}, args)

	query, values := build.SQL()
	err = sqlb.ScanRow(ctx, db, &task, query, values...)
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 1, Name: "the name", Age: 69}, task)
}

func TestInsertBuild(t *testing.T) {
	t.Parallel()

	task := Task{Name: "the name", Age: 32}

	build := sqlb.NewQuery("insert into tasks ?", sqlb.InsertSQL(task))
	query, args := build.SQL()

	be.Equal(t, "insert into tasks (name, age) VALUES (?, ?)", query)
	be.DeepEqual(t, []any{"the name", 32}, args)
}

func TestInsertBuildMany(t *testing.T) {
	t.Parallel()

	tasks := []Task{
		{Name: "a"},
		{Name: "b", Age: 1},
		{Name: "c", Age: 2},
	}

	build := sqlb.NewQuery("insert into tasks ?", sqlb.InsertSQL(tasks...))
	query, args := build.SQL()

	be.Equal(t, "insert into tasks (name, age) VALUES (?, ?), (?, ?), (?, ?)", query)
	be.DeepEqual(t, []any{"a", 0, "b", 1, "c", 2}, args)
}

func TestIter(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := t.Context()

	tasks := []Task{
		{Name: "a", Age: 1},
		{Name: "b", Age: 2},
		{Name: "c", Age: 3},
	}

	err := sqlb.Exec(ctx, db, "delete from tasks")
	be.NilErr(t, err)

	err = sqlb.Exec(ctx, db, "insert into tasks ?", sqlb.InsertSQL(tasks...))
	be.NilErr(t, err)

	next, stop := iter.Pull2(sqlb.Iter[Task](ctx, db, "select * from tasks order by age"))
	defer stop()

	task, err, ok := next()
	be.True(t, ok)
	be.NilErr(t, err)
	be.Equal(t, "a", task.Name)

	task, err, ok = next()
	be.True(t, ok)
	be.NilErr(t, err)
	be.Equal(t, "b", task.Name)

	task, err, ok = next()
	be.True(t, ok)
	be.NilErr(t, err)
	be.Equal(t, "c", task.Name)

	task, err, ok = next()
	be.False(t, ok)
	be.NilErr(t, err)
	be.Equal(t, Task{}, task)
}

func TestScan(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `insert into tasks (name) values (?)`, "one")
	be.NilErr(t, err)
	_, err = db.ExecContext(ctx, `insert into tasks (name) values (?)`, "two")
	be.NilErr(t, err)

	var out []*Task
	err = sqlb.ScanPtr(ctx, db, &out, "select * from tasks order by id")
	be.NilErr(t, err)
	be.DeepEqual(t, []*Task{
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
	}, out)

	var two Task
	err = sqlb.ScanRow(ctx, db, &two, "select * from tasks where id=?", 2)
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 2, Name: "two"}, two)
}

func TestExec(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := t.Context()

	task := Task{Name: "eg"}

	err := sqlb.Exec(ctx, db, "insert into tasks ?", sqlb.InsertSQL(task))
	be.NilErr(t, err)
}

func TestCollect(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `insert into tasks (name) values (?)`, "one")
	be.NilErr(t, err)
	_, err = db.ExecContext(ctx, `insert into tasks (name) values (?)`, "two")
	be.NilErr(t, err)

	t.Run("slice", func(t *testing.T) {
		t.Parallel()

		var out []int
		err := sqlb.Collect(ctx, db, sqlb.Slice(&out), "select id from tasks order by id")
		be.NilErr(t, err)
		be.DeepEqual(t, []int{1, 2}, out)
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()

		var out = map[int]struct{}{}
		err := sqlb.Collect(ctx, db, sqlb.Set(out), "select id from tasks order by id")
		be.NilErr(t, err)
		be.DeepEqual(t, map[int]struct{}{1: {}, 2: {}}, out)
	})
}

func TestHook(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	type hookData struct {
		typ, query string
		dur        time.Duration
	}

	var hooks []hookData
	ldb := sqlb.NewHookDB(db, func(ctx context.Context, typ, query string, dur time.Duration) {
		hooks = append(hooks, hookData{typ, query, dur})
	})

	var one int
	err := sqlb.ScanRow(ctx, ldb, sqlb.Values(&one), "select 1")
	be.NilErr(t, err)
	be.Equal(t, one, 1)

	err = sqlb.Exec(ctx, ldb, "select 0")
	be.NilErr(t, err)
	be.Equal(t, one, 1)

	var two int
	err = sqlb.ScanRow(ctx, ldb, sqlb.Values(&two), "select 2")
	be.NilErr(t, err)
	be.Equal(t, two, 2)

	be.Equal(t, len(hooks), 3)

	be.Equal(t, hooks[0].typ, "query")
	be.Equal(t, hooks[0].query, "select 1")
	be.True(t, hooks[0].dur > 0)

	be.Equal(t, hooks[1].typ, "exec")
	be.Equal(t, hooks[1].query, "select 0")
	be.True(t, hooks[1].dur > 0)

	be.Equal(t, hooks[2].typ, "query")
	be.Equal(t, hooks[2].query, "select 2")
	be.True(t, hooks[2].dur > 0)
}

func TestJSON(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	expBook := Book{
		Details: sqlb.NewJSON(map[string]any{
			"author": "abc",
			"date":   2025,
		}),
	}

	var id int
	err := sqlb.ScanRow(ctx, db, sqlb.Values(&id), `insert into books ? returning id`, sqlb.InsertSQL(expBook))
	be.NilErr(t, err)

	var book Book
	err = sqlb.ScanRow(ctx, db, &book, `select * from books where id=?`, id)
	be.NilErr(t, err)
	be.Equal(t, "abc", book.Details.Data["author"])
}

func TestStmtCache(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	var prepareCalls int
	cdb := sqlb.NewStmtCache(prepareWrap{db, func(ctx context.Context, query string) {
		t.Logf("prepared %q", query)
		prepareCalls++
	}})

	var one int
	err := sqlb.ScanRow(ctx, cdb, sqlb.Values(&one), "select 1 where ?", 1)
	be.NilErr(t, err)
	be.Equal(t, 1, one)

	be.Equal(t, 1, prepareCalls) // inc

	err = sqlb.ScanRow(ctx, cdb, sqlb.Values(&one), "select 1 where ?", 1)
	be.NilErr(t, err)
	be.Equal(t, 1, one)

	be.Equal(t, 1, prepareCalls) // unchanged

	err = sqlb.Exec(ctx, cdb, "select 2")
	be.NilErr(t, err)

	be.Equal(t, 2, prepareCalls) // inc

	tx, err := db.Begin()
	be.NilErr(t, err)

	err = sqlb.Exec(ctx, tx, "select 3")
	be.NilErr(t, err)

	err = tx.Commit()
	be.NilErr(t, err)

	be.Equal(t, 2, prepareCalls) // unchanged

	err = cdb.Close()
	be.NilErr(t, err)
}

type prepareWrap struct {
	*sql.DB
	cb func(ctx context.Context, query string)
}

func (pw prepareWrap) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	pw.cb(ctx, query)
	return pw.DB.PrepareContext(ctx, query)
}

func BenchmarkStmtCache(b *testing.B) {
	type bcase[T any] struct {
		name string
		v    T
	}
	dbs := []bcase[func(tb testing.TB) sqlb.ExecDB]{
		{"raw", func(tb testing.TB) sqlb.ExecDB { return newDB(tb) }},
		{"cached", func(tb testing.TB) sqlb.ExecDB { return sqlb.NewStmtCache(newDB(tb)) }},
	}
	queries := []bcase[sqlb.Query]{
		{"simple", sqlb.NewQuery(`select 1 where ? and ? and ? not in (?)`, 1, 1, 0, 4)},
		{"mid", sqlb.NewQuery(`select t1.id, t1.name, t1.age, (select count(*) from tasks where age > t1.age) as older_count from tasks t1 where t1.age > ? and t1.name like ? order by t1.age desc limit ?`, 30, "user%", 10)},
		{"complex", sqlb.NewQuery(`select t1.id, t1.name, t1.age, case when t1.age < 18 then 'minor' when t1.age < 30 then 'young adult' when t1.age < 50 then 'adult' else 'senior' end as age_group, (select count(*) from tasks t2 where case when t2.age < 18 then 'minor' when t2.age < 30 then 'young adult' when t2.age < 50 then 'adult' else 'senior' end = case when t1.age < 18 then 'minor' when t1.age < 30 then 'young adult' when t1.age < 50 then 'adult' else 'senior' end) as count_in_group from tasks t1 where t1.age between ? and ? and (t1.name like ? or t1.id % 10 = 0) order by t1.age desc, t1.name limit ? offset ?`, 25, 50, "user%", 20, 0)},
	}

	for _, db := range dbs {
		for _, query := range queries {
			b.Run(fmt.Sprintf("%s-%s", db.name, query.name), func(b *testing.B) {
				db := db.v(b)
				q, args := query.v.SQL()
				for b.Loop() {
					_, err := db.ExecContext(b.Context(), q, args...)
					be.NilErr(b, err)
				}
			})
		}
	}
}

func newDB(tb testing.TB) *sql.DB {
	tmpDir := tb.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "db"))
	be.NilErr(tb, err)
	tb.Cleanup(func() {
		_ = db.Close()
	})

	ctx := tb.Context()
	_, err = db.ExecContext(ctx, `create table tasks (id integer primary key autoincrement, name text not null default "", age integer not null default 0)`)
	be.NilErr(tb, err)
	_, err = db.ExecContext(ctx, `create table books (id integer primary key autoincrement, details json)`)
	be.NilErr(tb, err)

	return db
}

type Task struct {
	ID   int
	Name string
	Age  int
}

func (Task) PrimaryKey() string {
	return "id"
}

func (t Task) Values() []sql.NamedArg {
	return []sql.NamedArg{sql.Named("id", t.ID), sql.Named("name", t.Name), sql.Named("age", t.Age)}
}

func (t *Task) ScanFrom(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	dests := make([]any, 0, len(columns))
	for _, c := range columns {
		switch c {
		case "id":
			dests = append(dests, &t.ID)
		case "name":
			dests = append(dests, &t.Name)
		case "age":
			dests = append(dests, &t.Age)
		default:
			return fmt.Errorf("unknown column name %q", c)
		}
	}
	return rows.Scan(dests...)
}

type Book struct {
	ID      int
	Details sqlb.JSON[map[string]any]
}

func (Book) PrimaryKey() string {
	return "id"
}

func (b Book) Values() []sql.NamedArg {
	return []sql.NamedArg{sql.Named("id", b.ID), sql.Named("details", b.Details)}
}

func (b *Book) ScanFrom(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	dests := make([]any, 0, len(columns))
	for _, c := range columns {
		switch c {
		case "id":
			dests = append(dests, &b.ID)
		case "details":
			dests = append(dests, &b.Details)
		default:
			return fmt.Errorf("unknown column name %q", c)
		}
	}
	return rows.Scan(dests...)
}
