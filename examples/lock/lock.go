package main

import (
	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/lock"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	l := lock.NewLock(db)
	if err := l.CreateTable(); err != nil {
		panic(err)
	}

	l.State()
	l.TryLock()
	l.State()
	l.Unlock()
}
