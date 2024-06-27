package lock

import (
	"database/sql"
	"log/slog"
	"time"
)

const DefaultTableName = "barn_lock"
const DefaultNameField = "name"
const DefaultLockedAtField = "locked_at_ts"
const DefaultOwnerField = "owner"

type LockQueryConfig struct {
	TableName     string
	NameField     string
	LockedAtField string
	OwnerField    string
}

type Lock struct {
	Name     string
	LockedAt *time.Time
	Owner    *string
}

func (l Lock) LogValue() slog.Value {
	// return slog.AnyValue(computeExpensiveValue(e.arg))
	var args []slog.Attr
	args = append(args, slog.String("Name", l.Name))
	if l.LockedAt == nil {
		args = append(args, slog.Any("LockedAt", nil))
	} else {
		args = append(args, slog.Time("LockedAt", *l.LockedAt))
	}
	if l.Owner == nil {
		args = append(args, slog.Any("Owner", nil))
	} else {
		args = append(args, slog.String("Owner", *l.Owner))
	}
	return slog.GroupValue(args...)
}

type LockRepository interface {
	FindOne(tx *sql.Tx, name string) (*Lock, error)
	Create(tx *sql.Tx, name string) error
	Save(tx *sql.Tx, lock *Lock) error
}
