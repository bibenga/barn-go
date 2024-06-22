package lock

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type LeaderListener interface {
	OnElected()
	OnUnelected()
}

type LeaderElectorConfig struct {
	Log      *slog.Logger
	Lock     *Lock
	Listener LeaderListener
}

type LeaderElector struct {
	log      *slog.Logger
	lock     *Lock
	listener LeaderListener
}

func NewLeaderElector(config *LeaderElectorConfig) *LeaderElector {
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	if config.Lock == nil {
		panic(errors.New("lock is nil"))
	}
	if config.Listener == nil {
		config.Listener = &DummyLeaderListener{}
	}
	leader := LeaderElector{
		log:      config.Log,
		lock:     config.Lock,
		listener: config.Listener,
	}
	return &leader
}

func (l *LeaderElector) Start() {
	l.StartContext(context.Background())
}

func (l *LeaderElector) StartContext(ctx context.Context) {
	go l.Run(ctx)
}

func (l *LeaderElector) Run(ctx context.Context) {
	if err := l.open(); err != nil {
		panic(err)
	}

	hearbeat := time.NewTicker(l.lock.Hearbeat())
	defer hearbeat.Stop()

	l.log.Info("started")
	for {
		select {
		case <-ctx.Done():
			l.log.Info("terminate")
			if err := l.close(); err != nil {
				panic(err)
			}
			return
		case <-hearbeat.C:
			if err := l.hearbeat(); err != nil {
				panic(err)
			}
		}
	}
}

func (l *LeaderElector) open() error {
	if _, err := l.lock.Create(); err != nil {
		panic(err)
	}
	if l.lock.IsLocked() {
		l.onElected()
	} else {
		if locked, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if locked {
				l.onElected()
			}
		}
	}
	return nil
}

func (l *LeaderElector) hearbeat() error {
	if l.lock.IsLocked() {
		if confirmed, err := l.lock.Confirm(); err != nil {
			return err
		} else {
			if confirmed {
				l.log.Info("the lock is still owned me")
			} else {
				l.log.Warn("the lock has been acquired by someone unexpectedly")
				l.onUnelected()
			}
		}
	} else {
		if acquired, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if acquired {
				l.log.Info("the lock was rotten and it is acquired")
				l.onElected()
			} else {
				if state, err := l.lock.State(); err != nil {
					return err
				} else {
					l.log.Info("the lock state", "state", state)
				}
			}
		}
	}
	return nil
}

func (l *LeaderElector) close() error {
	if l.lock.IsLocked() {
		if unlocked, err := l.lock.Unlock(); err != nil {
			return err
		} else {
			if unlocked {
				l.onUnelected()
			}
		}
	}
	return nil
}

func (l *LeaderElector) onElected() {
	if l.listener != nil {
		l.listener.OnElected()
	}
}

func (l *LeaderElector) onUnelected() {
	if l.listener != nil {
		l.listener.OnUnelected()
	}
}

type DummyLeaderListener struct{}

func (l *DummyLeaderListener) OnElected() {
	slog.Info("DUMMY: I am a leader")
}

func (l *DummyLeaderListener) OnUnelected() {
	slog.Info("DUMMY: I am not a leader")
}

var _ LeaderListener = &DummyLeaderListener{}
