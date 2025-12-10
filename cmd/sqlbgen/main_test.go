package main

import (
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"sqlb": main,
	})
}

func TestScripts(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir:                 "testdata",
		RequireExplicitExec: true,
	})
}

func TestToSnake(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"Name", "name"},
		{"FirstName", "first_name"},
		{"UserID", "user_id"},
		{"JSON", "json"},
		{"UserJSON", "user_json"},
		{"A", "a"},
		{"", ""},
	}
	for _, c := range cases {
		if got := toSnake(c.in); got != c.want {
			t.Errorf("toSnake(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
