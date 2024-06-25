package lock

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type LeaderElectionHandler func(leader bool) error

type LeaderElectorConfig struct {
	Log     *slog.Logger
	Lock    *Locker
	Handler LeaderElectionHandler
}

type LeaderElector struct {
	log     *slog.Logger
	lock    *Locker
	handler LeaderElectionHandler
	running atomic.Bool
	cancel  context.CancelFunc
	stoped  sync.WaitGroup
}

func NewLeaderElector(config *LeaderElectorConfig) *LeaderElector {
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Lock == nil {
		panic(errors.New("lock is nil"))
	}
	if config.Handler == nil {
		config.Handler = dummyLeaderHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	leader := LeaderElector{
		log:     config.Log,
		lock:    config.Lock,
		handler: config.Handler,
	}
	return &leader
}

func (l *LeaderElector) Start() {
	l.StartContext(context.Background())
}

func (l *LeaderElector) StartContext(ctx context.Context) {
	if l.running.Load() {
		panic(errors.New("already running"))
	}

	l.stoped.Add(1)
	ctx, l.cancel = context.WithCancel(ctx)
	go l.run(ctx)
}

func (l *LeaderElector) Stop() {
	if !l.running.Load() {
		panic(errors.New("is not running"))
	}

	l.log.Debug("Stopping")
	l.cancel()
	l.stoped.Wait()
	l.log.Debug("Stopped")
}

func (l *LeaderElector) run(ctx context.Context) {
	l.log.Debug("leader elector stated")
	defer func() {
		l.log.Debug("leader elector terminated")
	}()

	l.running.Store(true)
	defer func() {
		l.running.Store(false)
	}()

	defer l.stoped.Done()

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
	if l.lock.IsLocked() {
		return errors.New("codebug: the lock is locked")
	}
	if locked, err := l.lock.TryLock(); err != nil {
		return err
	} else {
		if locked {
			return l.onElected()
		}
	}
	return nil
}

func (l *LeaderElector) hearbeat() error {
	if l.lock.IsLocked() {
		if confirmed, err := l.lock.Confirm(); err != nil {
			return err
		} else {
			if !confirmed {
				return l.onUnelected()
			}
		}
	} else {
		if acquired, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if acquired {
				l.log.Info("the lock was rotten and it is acquired")
				return l.onElected()
			} else {
				// l.log.Info("the lock was rotten and it is acquired")
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
				return l.onUnelected()
			}
		}
	}
	return nil
}

func (l *LeaderElector) onElected() error {
	return l.handler(true)
}

func (l *LeaderElector) onUnelected() error {
	return l.handler(false)
}

func dummyLeaderHandler(leader bool) error {
	if leader {
		slog.Info("DUMMY: I am a leader")
	} else {
		slog.Info("DUMMY: I am not a leader")
	}
	return nil
}
