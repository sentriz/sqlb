package sqlb_test

import (
	"context"
	"database/sql"
	"testing"

	"go.senan.xyz/sqlb"

	"github.com/carlmjohnson/be"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func TestBuild(t *testing.T) {
	var b sqlb.Query
	b.Append("select * from jobs")
	b.Append("where 1")
	b.Append("and one=?", 1)
	b.Append("and two=?", 2)
	b.Append("returning *")

	query, args := b.SQL()
	be.Equal(t, "select * from jobs where 1 and one=? and two=? returning *", query)
	be.DeepEqual(t, []any{1, 2}, args)
}

func TestBuildSubquery(t *testing.T) {
	var whereA sqlb.Query
	whereA.Append("three=?", 3)

	whereB := sqlb.NewQuery("four=?", 4)

	var whereC sqlb.Query
	whereC.Append("? or ?", whereA, whereB)

	where := sqlb.NewQuery("?", whereC)

	var b sqlb.Query
	b.Append("select * from ? union ?",
		sqlb.SubQuery("select * from jobs where a=?",
			"a",
		),
		sqlb.SubQuery("select * from jobs where a=? and ?",
			"aa",
			sqlb.NewQuery("xx=?", 10),
		),
	)
	b.Append("where ?", where)

	query, args := b.SQL()
	be.Equal(t, "select * from (select * from jobs where a=?) union (select * from jobs where a=? and xx=?) where three=? or four=?", query)
	be.DeepEqual(t, []any{"a", "aa", 10, 3, 4}, args)
}

func TestBuildPanic(t *testing.T) {
	expPanic := "want 3 args, got 2"

	defer func() {
		if r := recover(); r != expPanic {
			t.Errorf("exp panic %q got %q", expPanic, r)
		}
	}()

	var b sqlb.Query
	b.Append("one=?, two=?, three=?", 1, 2)
}

func TestUpdate(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()

	var task Task
	err := sqlb.ScanRow(ctx, db, &task, "insert into tasks (name) values (?) returning *", "the name")
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 3, Name: "the name", Age: 0}, task)

	task.Age = 69

	build := sqlb.NewQuery("update tasks set ? where id=? returning *", sqlb.UpdateSQL(task), task.ID)
	query, args := build.SQL()

	be.Equal(t, "update tasks set name=? , age=? where id=? returning *", query)
	be.DeepEqual(t, []any{"the name", 69, 3}, args)

	query, values := build.SQL()
	err = sqlb.ScanRow(ctx, db, &task, query, values...)
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 3, Name: "the name", Age: 69}, task)
}

func TestInsertBuild(t *testing.T) {
	task := Task{Name: "the name", Age: 32}

	build := sqlb.NewQuery("insert into tasks ?", sqlb.InsertSQL(task))
	query, args := build.SQL()

	be.Equal(t, "insert into tasks (name, age) VALUES (?, ?)", query)
	be.DeepEqual(t, []any{"the name", 32}, args)

}

func TestInsert(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()

	task := Task{Name: "the name", Age: 32}
	err := sqlb.ScanRow(ctx, db, &task, "insert into tasks ? returning *", sqlb.InsertSQL(task))
	be.NilErr(t, err)
	be.DeepEqual(t, Task{ID: 3, Name: "the name", Age: 32}, task)
}

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
func (t *Task) ScanFrom(rows *sql.Rows) error {
	return rows.Scan(&t.ID, &t.Name, &t.Age)
}

func TestScan(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()

	var out []*Task
	err := sqlb.Scan(ctx, db, &out, "select * from tasks order by id")
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

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	be.NilErr(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `create table tasks (id integer primary key autoincrement, name text not null default "", age integer not null default 0)`)
	be.NilErr(t, err)
	_, err = db.ExecContext(ctx, `insert into tasks (name) values (?)`, "one")
	be.NilErr(t, err)
	_, err = db.ExecContext(ctx, `insert into tasks (name) values (?)`, "two")
	be.NilErr(t, err)

	return db
}
