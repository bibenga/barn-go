package lock

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type LeaderHandler func()
type LeaderListener interface {
	OnElected()
	OnUnelected()
}

type LeaderElectorConfig struct {
	Log                *slog.Logger
	Lock               *Lock
	Listener           LeaderListener
	OnElectedHandler   LeaderHandler
	OnUnelectedHandler LeaderHandler
}

type LeaderElector struct {
	log                *slog.Logger
	lock               *Lock
	listener           LeaderListener
	onElectedHandler   LeaderHandler
	onUnelectedHandler LeaderHandler
	running            atomic.Bool
	cancel             context.CancelFunc
	stoped             sync.WaitGroup
}

func NewLeaderElector(config *LeaderElectorConfig) *LeaderElector {
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Lock == nil {
		panic(errors.New("lock is nil"))
	}
	if config.Listener == nil {
		config.Listener = &DummyLeaderListener{}
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	leader := LeaderElector{
		log:                config.Log,
		lock:               config.Lock,
		listener:           config.Listener,
		onElectedHandler:   config.OnElectedHandler,
		onUnelectedHandler: config.OnUnelectedHandler,
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
			if !confirmed {
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
	if l.onElectedHandler != nil {
		l.onElectedHandler()
	}
}

func (l *LeaderElector) onUnelected() {
	if l.listener != nil {
		l.listener.OnUnelected()
	}
	if l.onUnelectedHandler != nil {
		l.onUnelectedHandler()
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
