package lock

import (
	"log/slog"
	"time"
)

type Leader struct {
	log      *slog.Logger
	lock     *Lock
	listener LeaderListener
	stop     chan struct{}
	stopped  chan struct{}
}

type LeaderListener interface {
	OnLock()
	OnUnlock()
}

func NewLeader(lock *Lock, listener LeaderListener) *Leader {
	leader := Leader{
		log:      slog.Default(),
		lock:     lock,
		listener: listener,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	return &leader
}

func (l *Leader) Start() error {
	go l.Run()
	return nil
}

func (l *Leader) Stop() {
	l.log.Info("stopping")
	l.stop <- struct{}{}
	<-l.stopped
	close(l.stop)
	close(l.stopped)
	l.log.Info("stopped")
}

func (l *Leader) Run() {
	if err := l.open(); err != nil {
		panic(err)
	}

	hearbeat := time.NewTicker(l.lock.Hearbeat())
	defer hearbeat.Stop()

	l.log.Info("started")
	for {
		select {
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

func (l *Leader) open() error {
	if err := l.lock.Create(); err != nil {
		panic(err)
	}
	if l.lock.IsLocked() {
		l.onLock()
	} else {
		if locked, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if locked {
				l.onLock()
			}
		}
	}
	return nil
}

func (l *Leader) hearbeat() error {
	if l.lock.IsLocked() {
		if confirmed, err := l.lock.Confirm(); err != nil {
			return err
		} else {
			if confirmed {
				l.log.Info("the lock is still owned me")
			} else {
				l.log.Warn("the lock has been acquired by someone unexpectedly")
				l.onUnlock()
			}
		}
	} else {
		if acquired, err := l.lock.TryLock(); err != nil {
			return err
		} else {
			if acquired {
				l.log.Info("the lock is rotten and acquired")
				l.onLock()
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

func (l *Leader) close() error {
	if l.lock.IsLocked() {
		if unlocked, err := l.lock.Unlock(); err != nil {
			return err
		} else {
			if unlocked {
				l.onUnlock()
			}
		}
	}
	return nil
}

func (l *Leader) onLock() {
	if l.listener != nil {
		l.listener.OnLock()
	}
}

func (l *Leader) onUnlock() {
	if l.listener != nil {
		l.listener.OnUnlock()
	}
}

type DummyLeaderListener struct{}

func (l *DummyLeaderListener) OnLock() {
	slog.Info("DUMMY: the lock is acquired")
}

func (l *DummyLeaderListener) OnUnlock() {
	slog.Info("DUMMY: the lock is released")
}

var _ LeaderListener = &DummyLeaderListener{}
