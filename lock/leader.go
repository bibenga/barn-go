package lock

import (
	"context"
	"log/slog"
	"time"
)

type LeaderElector struct {
	log      *slog.Logger
	lock     *Lock
	listener LeaderListener
	stop     chan struct{}
	stopped  chan struct{}
}

type LeaderListener interface {
	OnHire()
	OnFire()
}

func NewLeaderElector(lock *Lock, listener LeaderListener) *LeaderElector {
	leader := LeaderElector{
		log:      slog.Default(),
		lock:     lock,
		listener: listener,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	return &leader
}

func (l *LeaderElector) Start() {
	l.StartContext(context.Background())
}

func (l *LeaderElector) StartContext(ctx context.Context) {
	go l.Run(ctx)
}

func (l *LeaderElector) Stop() {
	l.log.Debug("stopping")
	l.stop <- struct{}{}
	<-l.stopped
	close(l.stop)
	close(l.stopped)
	l.log.Info("stopped")
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
		case <-l.stop:
			l.log.Info("terminate")
			if err := l.close(); err != nil {
				panic(err)
			}
			l.stopped <- struct{}{}
			return
		case <-hearbeat.C:
			if err := l.hearbeat(); err != nil {
				panic(err)
			}
		}
	}
}

func (l *LeaderElector) open() error {
	if err := l.lock.Create(); err != nil {
		panic(err)
	}
	if l.lock.IsLocked() {
		l.onHire()
	} else {
		if locked, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if locked {
				l.onHire()
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
				l.onFire()
			}
		}
	} else {
		if acquired, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if acquired {
				l.log.Info("the lock is rotten and acquired")
				l.onHire()
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
				l.onFire()
			}
		}
	}
	return nil
}

func (l *LeaderElector) onHire() {
	if l.listener != nil {
		l.listener.OnHire()
	}
}

func (l *LeaderElector) onFire() {
	if l.listener != nil {
		l.listener.OnFire()
	}
}

type DummyLeaderListener struct{}

func (l *DummyLeaderListener) OnHire() {
	slog.Info("DUMMY: the leader is hired")
}

func (l *DummyLeaderListener) OnFire() {
	slog.Info("DUMMY: the leader is fired")
}

var _ LeaderListener = &DummyLeaderListener{}
