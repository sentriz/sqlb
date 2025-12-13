package sqlb_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"go.senan.xyz/sqlb"
)

func ExampleQuery() {
	var q sqlb.Query
	q.Append("SELECT * FROM users WHERE 1")
	q.Append("AND name = ?", "alice")
	q.Append("AND age > ?", 18)

	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE 1 AND name = ? AND age > ?
	// [alice 18]
}

func ExampleQuery_subquery() {
	subquery := sqlb.NewQuery("SELECT id FROM admins WHERE level > ?", 5)

	var q sqlb.Query
	q.Append("SELECT * FROM users WHERE id IN (?)", subquery)

	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE id IN (SELECT id FROM admins WHERE level > ?)
	// [5]
}

func ExampleQuery_nested() {
	var whereA sqlb.Query
	whereA.Append("three=?", 3)

	whereB := sqlb.NewQuery("four=?", 4)

	var whereC sqlb.Query
	whereC.Append("? or ?", whereA, whereB)

	where := sqlb.NewQuery("?", whereC)

	var b sqlb.Query
	b.Append("select * from (?) union (?)",
		sqlb.NewQuery("select * from tasks where a=?", "a"),
		sqlb.NewQuery("select * from tasks where a=? and ?", "aa", sqlb.NewQuery("xx=?", 10)),
	)
	b.Append("where ?", where)

	query, args := b.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// select * from (select * from tasks where a=?) union (select * from tasks where a=? and xx=?) where three=? or four=?
	// [a aa 10 3 4]
}

func ExampleNewQuery() {
	q := sqlb.NewQuery("SELECT * FROM users WHERE name = ?", "bob")
	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE name = ?
	// [bob]
}

func TestQueryPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != "want 3 args, got 2" {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	var b sqlb.Query
	b.Append("one=?, two=?, three=?", 1, 2)
}

func ExampleUpdateSQL() {
	task := Task{ID: 1, Name: "alice", Age: 31}

	q := sqlb.NewQuery("UPDATE tasks SET ? WHERE id = ?", sqlb.UpdateSQL(task), task.ID)
	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// UPDATE tasks SET name=? , age=? WHERE id = ?
	// [alice 31 1]
}

func ExampleInsertSQL() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	task := Task{Name: "alice", Age: 30}
	if err := sqlb.ScanRow(ctx, db, &task, "INSERT INTO tasks ? RETURNING *", sqlb.InsertSQL(task)); err != nil {
		panic(err)
	}
	fmt.Println(task.ID, task.Name, task.Age)
	// Output:
	// 1 alice 30
}

func ExampleInsertSQL_many() {
	tasks := []Task{
		{Name: "alice", Age: 30},
		{Name: "bob", Age: 25},
	}

	q := sqlb.NewQuery("INSERT INTO tasks ?", sqlb.InsertSQL(tasks...))
	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// INSERT INTO tasks (name, age) VALUES (?, ?), (?, ?)
	// [alice 30 bob 25]
}

func TestInsertSQLPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != "InsertSQL called with zero arguments" {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	sqlb.InsertSQL[Task]()
}

func ExampleInSQL() {
	ids := []int{1, 2, 3}

	var q sqlb.Query
	q.Append("SELECT * FROM users WHERE id IN ?", sqlb.InSQL(ids...))

	query, args := q.SQL()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE id IN (?, ?, ?)
	// [1 2 3]
}

func TestInSQLPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != "InSQL called with zero arguments" {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	sqlb.InSQL[int]()
}

func ExampleScanRow() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "alice", Age: 30}))

	var task Task
	if err := sqlb.ScanRow(ctx, db, &task, "SELECT * FROM tasks WHERE name = ?", "alice"); err != nil {
		panic(err)
	}
	fmt.Println(task.Name, task.Age)
	// Output:
	// alice 30
}

func TestScanRowNoRows(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	var task Task
	err := sqlb.ScanRow(ctx, db, &task, "SELECT * FROM tasks WHERE id = ?", 999)
	if err != sql.ErrNoRows {
		t.Errorf("got %v, want sql.ErrNoRows", err)
	}
}

func TestScanRowQueryError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	var task Task
	err := sqlb.ScanRow(ctx, db, &task, "SELECT * FROM nonexistent")
	if err == nil {
		t.Error("expected error for invalid table")
	}
}

func ExampleScanRows() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(
		Task{Name: "alice", Age: 30},
		Task{Name: "bob", Age: 25},
		Task{Name: "carol", Age: 35},
	))

	var tasks []Task
	if err := sqlb.ScanRows(ctx, db, sqlb.Append(&tasks), "SELECT * FROM tasks ORDER BY name"); err != nil {
		panic(err)
	}
	for _, t := range tasks {
		fmt.Println(t.Name, t.Age)
	}
	// Output:
	// alice 30
	// bob 25
	// carol 35
}

func TestScanRowsQueryError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	var tasks []Task
	err := sqlb.ScanRows(ctx, db, sqlb.Append(&tasks), "SELECT * FROM nonexistent")
	if err == nil {
		t.Error("expected error for invalid table")
	}
}

func TestScanRowsScanError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "one"}))

	var names []string
	err := sqlb.ScanRows(ctx, db, sqlb.AppendValue(&names), "SELECT id, name FROM tasks")
	if err == nil {
		t.Error("expected scan error for wrong column count")
	}
}

func ExampleIterRows() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(
		Task{Name: "alice", Age: 30},
		Task{Name: "bob", Age: 25},
	))

	for task, err := range sqlb.IterRows[Task](ctx, db, "SELECT * FROM tasks ORDER BY name") {
		if err != nil {
			panic(err)
		}
		fmt.Println(task.Name, task.Age)
	}
	// Output:
	// alice 30
	// bob 25
}

func TestIterRowsQueryError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	for _, err := range sqlb.IterRows[Task](ctx, db, "SELECT * FROM nonexistent") {
		if err == nil {
			t.Error("expected error for invalid table")
		}
		return
	}
	t.Error("expected at least one iteration")
}

func TestIterRowsScanError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "one"}))

	for _, err := range sqlb.IterRows[Task](ctx, db, "SELECT id, name, age, 'extra' as extra FROM tasks") {
		if err == nil {
			t.Error("expected scan error for unknown column")
		}
		return
	}
}

func ExampleExec() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	if err := sqlb.Exec(ctx, db, "INSERT INTO tasks (name) VALUES (?)", "alice"); err != nil {
		panic(err)
	}
	fmt.Println("inserted")
	// Output:
	// inserted
}

func ExampleAppend() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "one"}, Task{Name: "two"}))

	var tasks []Task
	if err := sqlb.ScanRows(ctx, db, sqlb.Append(&tasks), "SELECT * FROM tasks ORDER BY id"); err != nil {
		panic(err)
	}
	fmt.Println(len(tasks))
	fmt.Println(tasks[0].Name)
	// Output:
	// 2
	// one
}

func ExampleAppendPtr() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "one"}, Task{Name: "two"}))

	var tasks []*Task
	if err := sqlb.ScanRows(ctx, db, sqlb.AppendPtr(&tasks), "SELECT * FROM tasks ORDER BY id"); err != nil {
		panic(err)
	}
	fmt.Println(len(tasks))
	fmt.Println(tasks[0].Name)
	// Output:
	// 2
	// one
}

func ExampleValues() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	var x, y int
	if err := sqlb.ScanRow(ctx, db, sqlb.Values(&x, &y), "SELECT 10, 20"); err != nil {
		panic(err)
	}
	fmt.Println(x, y)
	// Output:
	// 10 20
}

func ExampleAppendValue() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(
		Task{Name: "alice"},
		Task{Name: "bob"},
		Task{Name: "carol"},
	))

	var names []string
	if err := sqlb.ScanRows(ctx, db, sqlb.AppendValue(&names), "SELECT name FROM tasks ORDER BY name"); err != nil {
		panic(err)
	}
	fmt.Println(names)
	// Output:
	// [alice bob carol]
}

func ExampleSetValue() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(
		Task{Name: "alice"},
		Task{Name: "bob"},
		Task{Name: "alice"},
	))

	names := make(map[string]struct{})
	if err := sqlb.ScanRows(ctx, db, sqlb.SetValue(names), "SELECT name FROM tasks"); err != nil {
		panic(err)
	}
	fmt.Println(len(names))
	// Output:
	// 2
}

func ExampleJSON() {
	ctx := context.Background()
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	_ = sqlb.Exec(ctx, db, `CREATE TABLE config (id INTEGER PRIMARY KEY, data JSON)`)

	config := sqlb.NewJSON(map[string]string{"theme": "dark", "lang": "en"})
	_ = sqlb.Exec(ctx, db, `INSERT INTO config (data) VALUES (?)`, config)

	var data sqlb.JSON[map[string]string]
	_ = sqlb.ScanRow(ctx, db, sqlb.Values(&data), "SELECT data FROM config LIMIT 1")
	fmt.Println(data.Data["theme"])
	// Output:
	// dark
}

func TestJSONScanNull(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, `INSERT INTO books (details) VALUES (NULL)`)

	var data sqlb.JSON[map[string]any]
	err := sqlb.ScanRow(ctx, db, sqlb.Values(&data), "SELECT details FROM books LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}
	if data.Data != nil {
		t.Errorf("expected nil data for NULL column, got %v", data.Data)
	}
}

func TestJSONScanString(t *testing.T) {
	// Test that JSON.Scan handles string type (some drivers return string instead of []byte)
	var data sqlb.JSON[map[string]string]
	err := data.Scan(`{"key":"value"}`)
	if err != nil {
		t.Fatal(err)
	}
	if data.Data["key"] != "value" {
		t.Errorf("expected key=value, got %v", data.Data)
	}
}

func TestJSONScanInvalidType(t *testing.T) {
	var data sqlb.JSON[map[string]any]
	err := data.Scan(123)
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

func ExampleWithLogFunc() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	ctx = sqlb.WithLogFunc(ctx, func(ctx context.Context, typ, query string, dur time.Duration) {
		fmt.Printf("type=%s query=%s\n", typ, query)
	})

	var x int
	_ = sqlb.ScanRow(ctx, db, sqlb.Values(&x), "SELECT 42")
	// Output:
	// type=query query=SELECT 42
}

func TestLog(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	type hookData struct {
		typ, query string
		dur        time.Duration
	}

	var hooks []hookData
	ctx = sqlb.WithLogFunc(ctx, func(ctx context.Context, typ, query string, dur time.Duration) {
		hooks = append(hooks, hookData{typ, query, dur})
	})

	var one int
	if err := sqlb.ScanRow(ctx, db, sqlb.Values(&one), "select 1"); err != nil {
		t.Fatal(err)
	}
	if one != 1 {
		t.Errorf("got %d, want 1", one)
	}

	if err := sqlb.Exec(ctx, db, "select 0"); err != nil {
		t.Fatal(err)
	}

	var two int
	if err := sqlb.ScanRow(ctx, db, sqlb.Values(&two), "select 2"); err != nil {
		t.Fatal(err)
	}
	if two != 2 {
		t.Errorf("got %d, want 2", two)
	}

	if len(hooks) != 3 {
		t.Fatalf("got %d hooks, want 3", len(hooks))
	}

	if hooks[0].typ != "query" || hooks[0].query != "select 1" || hooks[0].dur <= 0 {
		t.Errorf("unexpected hook[0]: %+v", hooks[0])
	}
	if hooks[1].typ != "exec" || hooks[1].query != "select 0" || hooks[1].dur <= 0 {
		t.Errorf("unexpected hook[1]: %+v", hooks[1])
	}
	if hooks[2].typ != "query" || hooks[2].query != "select 2" || hooks[2].dur <= 0 {
		t.Errorf("unexpected hook[2]: %+v", hooks[2])
	}
}

func TestLogScanRows(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	var logged bool
	ctx = sqlb.WithLogFunc(ctx, func(ctx context.Context, typ, query string, dur time.Duration) {
		logged = true
	})

	var tasks []Task
	if err := sqlb.ScanRows(ctx, db, sqlb.Append(&tasks), "SELECT * FROM tasks"); err != nil {
		t.Fatal(err)
	}
	if !logged {
		t.Error("expected log to be called")
	}
}

func TestLogIterRows(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	_ = sqlb.Exec(ctx, db, "INSERT INTO tasks ?", sqlb.InsertSQL(Task{Name: "one"}))

	var logged bool
	ctx = sqlb.WithLogFunc(ctx, func(ctx context.Context, typ, query string, dur time.Duration) {
		logged = true
	})

	for task, err := range sqlb.IterRows[Task](ctx, db, "SELECT * FROM tasks") {
		if err != nil {
			t.Fatal(err)
		}
		_ = task
	}
	if !logged {
		t.Error("expected log to be called")
	}
}

func ExampleStmtCache() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	cache := sqlb.NewStmtCache(db)
	defer cache.Close()

	var x int
	_ = sqlb.ScanRow(ctx, cache, sqlb.Values(&x), "SELECT 1")
	_ = sqlb.ScanRow(ctx, cache, sqlb.Values(&x), "SELECT 1") // uses cached statement
	fmt.Println(x)
	// Output:
	// 1
}

func TestStmtCache(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	var prepareCalls int
	cdb := sqlb.NewStmtCache(prepareWrap{db, func(ctx context.Context, query string) {
		prepareCalls++
	}})

	var one int
	if err := sqlb.ScanRow(ctx, cdb, sqlb.Values(&one), "select 1 where ?", 1); err != nil {
		t.Fatal(err)
	}
	if one != 1 {
		t.Errorf("got %d, want 1", one)
	}
	if prepareCalls != 1 {
		t.Errorf("got %d prepareCalls, want 1", prepareCalls)
	}

	if err := sqlb.ScanRow(ctx, cdb, sqlb.Values(&one), "select 1 where ?", 1); err != nil {
		t.Fatal(err)
	}
	if prepareCalls != 1 {
		t.Errorf("got %d prepareCalls, want 1 (should be cached)", prepareCalls)
	}

	if err := sqlb.Exec(ctx, cdb, "select 2"); err != nil {
		t.Fatal(err)
	}
	if prepareCalls != 2 {
		t.Errorf("got %d prepareCalls, want 2", prepareCalls)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlb.Exec(ctx, tx, "select 3"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if prepareCalls != 2 {
		t.Errorf("got %d prepareCalls, want 2 (tx doesn't use cache)", prepareCalls)
	}

	if err := cdb.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStmtCachePrepareError(t *testing.T) {
	ctx := t.Context()
	db := newDB(ctx)
	defer db.Close()

	cache := sqlb.NewStmtCache(db)
	defer cache.Close()

	var x int
	if err := sqlb.ScanRow(ctx, cache, sqlb.Values(&x), "INVALID SQL"); err == nil {
		t.Error("expected error for invalid SQL")
	}

	if err := sqlb.Exec(ctx, cache, "ANOTHER INVALID SQL"); err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func BenchmarkStmtCache(b *testing.B) {
	ctx := b.Context()

	type bcase[T any] struct {
		name string
		v    T
	}
	dbs := []bcase[func(tb testing.TB) sqlb.ExecDB]{
		{"raw", func(tb testing.TB) sqlb.ExecDB { return newDB(ctx) }},
		{"cached", func(tb testing.TB) sqlb.ExecDB { return sqlb.NewStmtCache(newDB(b.Context())) }},
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
					if _, err := db.ExecContext(b.Context(), q, args...); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func Example_cRUD() {
	ctx := context.Background()
	db := newDB(ctx)
	defer db.Close()

	task := Task{Name: "alice", Age: 30}
	if err := sqlb.ScanRow(ctx, db, &task, "INSERT INTO tasks ? RETURNING *", sqlb.InsertSQL(task)); err != nil {
		panic(err)
	}
	fmt.Println("inserted:", task.ID, task.Name, task.Age)

	task.Age = 31
	if err := sqlb.ScanRow(ctx, db, &task, "UPDATE tasks SET ? WHERE id = ? RETURNING *", sqlb.UpdateSQL(task), task.ID); err != nil {
		panic(err)
	}
	fmt.Println("updated:", task.ID, task.Name, task.Age)

	var readTask Task
	if err := sqlb.ScanRow(ctx, db, &readTask, "SELECT * FROM tasks WHERE id = ?", task.ID); err != nil {
		panic(err)
	}
	fmt.Println("read:", readTask.ID, readTask.Name, readTask.Age)
	// Output:
	// inserted: 1 alice 30
	// updated: 1 alice 31
	// read: 1 alice 31
}

type prepareWrap struct {
	*sql.DB
	cb func(ctx context.Context, query string)
}

func (pw prepareWrap) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	pw.cb(ctx, query)
	return pw.DB.PrepareContext(ctx, query)
}

func newDB(ctx context.Context) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	if err := sqlb.Exec(ctx, db, `create table tasks (id integer primary key autoincrement, name text not null default "", age integer not null default 0)`); err != nil {
		panic(err)
	}
	if err := sqlb.Exec(ctx, db, `create table books (id integer primary key autoincrement, details json)`); err != nil {
		panic(err)
	}
	return db
}

type Task struct {
	ID   int
	Name string
	Age  int
}

func (Task) IsGenerated(c string) bool {
	return c == "id"
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

func (Book) IsGenerated(c string) bool {
	return c == "id"
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
