package sqlb_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.senan.xyz/sqlb"

	"github.com/carlmjohnson/be"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func TestBuild(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

func TestUpdate(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	task := Task{Name: "the name", Age: 32}

	build := sqlb.NewQuery("insert into tasks ?", sqlb.InsertSQL(task))
	query, args := build.SQL()

	be.Equal(t, "insert into tasks (name, age) VALUES (?, ?)", query)
	be.DeepEqual(t, []any{"the name", 32}, args)

}

func TestInsert(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

type JobStatus string

const (
	StatusEnqueued   JobStatus = ""
	StatusInProgress JobStatus = "in-progress"
	StatusNeedsInput JobStatus = "needs-input"
	StatusError      JobStatus = "error"
	StatusComplete   JobStatus = "complete"
)

type Operation string

const (
	OperationCopy Operation = "copy"
	OperationMove Operation = "move"
)

type SearchResult struct {
	Score   float64
	DestDir string
	Diff    []string
}

type Job struct {
	ID                   uint64
	Status               JobStatus
	Error                string
	Operation            Operation
	Time                 time.Time
	UseMBID              string
	SourcePath, DestPath string
	SearchResult         sqlb.JSON[*SearchResult]
}

func (Job) PrimaryKey() string {
	return "id"
}
func (j Job) Values() []sql.NamedArg {
	return []sql.NamedArg{
		sql.Named("id", j.ID),
		sql.Named("status", j.Status),
		sql.Named("error", j.Error),
		sql.Named("operation", j.Operation),
		sql.Named("time", j.Time),
		sql.Named("use_mbid", j.UseMBID),
		sql.Named("source_path", j.SourcePath),
		sql.Named("dest_path", j.DestPath),
		sql.Named("search_result", j.SearchResult),
	}
}
func (j *Job) ScanFrom(rows *sql.Rows) error {
	return rows.Scan(&j.ID, &j.Status, &j.Error, &j.Operation, &j.Time, &j.UseMBID, &j.SourcePath, &j.DestPath, &j.SearchResult)
}

func jobsMigrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		create table if not exists jobs (
			id            integer primary key autoincrement,
			status        text not null default "",
			error         text not null default "",
			operation     text not null,
			time          timestamp not null,
			use_mbid      text not null default "",
			source_path   text not null,
			dest_path     text not null default "",
			search_result jsonb
		);

		create index if not exists idx_jobs_status on jobs (status);
		create index if not exists idx_jobs_source_path on jobs (source_path);
	`)
	return err
}

func TestInsertJob(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := context.Background()

	err := jobsMigrate(ctx, db)
	be.NilErr(t, err)

	job := Job{
		Status:     StatusInProgress,
		Operation:  OperationMove,
		Time:       time.Now(),
		SourcePath: "/some/path",
		SearchResult: sqlb.JSON[*SearchResult]{&SearchResult{
			Score:   100,
			DestDir: "some dir",
			Diff:    []string{"one", "two"},
		}},
	}

	err = sqlb.ScanRow(ctx, db, &job, "insert into jobs ? returning *", sqlb.InsertSQL(job))
	be.NilErr(t, err)
	be.Nonzero(t, job.ID)

	job.Status = StatusComplete
	err = sqlb.ScanRow(ctx, db, &job, "update jobs set ? returning *", sqlb.UpdateSQL(job))
	be.NilErr(t, err)

	var readJob Job
	err = sqlb.ScanRow(ctx, db, &readJob, "select * from jobs where id = ?", job.ID)
	be.NilErr(t, err)

	be.DeepEqual(t, job, readJob)
}

func TestExec(t *testing.T) {
	t.Parallel()

	db := newDB(t)
	ctx := context.Background()

	task := Task{Name: "eg"}

	err := sqlb.Exec(ctx, db, "insert into tasks ?", sqlb.InsertSQL(task))
	be.NilErr(t, err)
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
