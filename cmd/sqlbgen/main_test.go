package main

import (
	"testing"

	"github.com/carlmjohnson/be"
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
	be.Equal(t, "name", toSnake("Name"))
	be.Equal(t, "first_name", toSnake("FirstName"))
	be.Equal(t, "user_id", toSnake("UserID"))
	be.Equal(t, "json", toSnake("JSON"))
	be.Equal(t, "user_json", toSnake("UserJSON"))
	be.Equal(t, "a", toSnake("A"))
	be.Equal(t, "", toSnake(""))
}
