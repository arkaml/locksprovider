package locksprovider

import (
	"fmt"
	"time"
)

const LOCK_ALREADY_RELEASED = "lock already released"

type AutoRenewFunc func()

type Lock struct {
	id string

	expired      bool
	expiredMutex chan bool

	release       chan string
	releaseCancel func()
}

func NewLock(id string, ttl ...time.Duration) (Lock, error) {
	release := make(chan string, 1)
	expiredMutex := make(chan bool, 1)

	return Lock{id, false, expiredMutex, release, nil}, nil
}

func (this *Lock) Release() error {

	// double-checked locking

	if this.expired {
		return fmt.Errorf(LOCK_ALREADY_RELEASED)
	}

	this.expiredMutex <- true
	if this.expired {
		// we are checking expired instead of just relying on being able to write on
		// release channel as i want to avoid more than one write on release channel,
		// for how its designed, this mutex lock should not hit overall performance
		// (unless you are sharing a same Lock instance between multiple threads)

		<-this.expiredMutex
		return fmt.Errorf(LOCK_ALREADY_RELEASED)
	}

	this.expired = true
	<-this.expiredMutex

	// write over release channel will be performed only once per

	this.release <- this.id
	return nil
}

func (this *Lock) AutoRenew(ttl time.Duration) error {
	if this.releaseCancel != nil {
		this.releaseCancel()
	}

	return nil
}

func (this *Lock) IsValid() bool {

	// double-checked locking

	if this.expired {
		return false
	}

	this.expiredMutex <- true
	defer func() { <-this.expiredMutex }()

	return !this.expired
}
