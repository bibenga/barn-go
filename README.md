## Task pool and scheduler using database

This is a simple scheduler with a database store (it was tested only with PostgreSQL)

### Using the task pool

### Using the scheduler
```go
package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"

	"github.com/bibenga/barn-go/scheduler"
)

func main() {
	db, err := sql.Open("pgx", "host=rds port=5432 user=rds password=sqlsql dbname=rds TimeZone=UTC sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := scheduler.NewDefaultPostgresSchedulerRepository()

	ctx, cancel := context.WithCancel(context.Background())

	sched := scheduler.NewScheduler(db, &scheduler.SchedulerConfig{Repository: repository})
	sched.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	
    cancel()
    sched.Stop()
}

```
