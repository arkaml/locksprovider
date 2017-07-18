package locksprovider

import (
	"fmt"
	"reflect"
	"time"
)

// package internal use constants

const TUNNELS_REQUEST_CHANNEL = 0
const TUNNELS_CLEANUP_CHANNEL = 1

// package configuration constants

const LOCKS_ACQUIRING_TIMEOUT = time.Second * 5
const TUNNELS_CLEANUP_TIMEOUT = time.Second * 5
const LOCK_TIMEOUT_ERROR = "could not acquire Lock for resource %s in the specified Provider acquiring locks timeout"

type LockTunnel struct {
	current Lock
	pending uint32
	channel chan Lock
}

type LockProvider struct {
	timeout time.Duration
	tunnels map[string]*LockTunnel

	tunnelsCleanup  *time.Ticker
	tunnelsRequest  chan string
	tunnelsResponse chan LockTunnel
}

var str []string

func NewProvider(timeout ...time.Duration) *LockProvider {

	// set locks allocation timeout

	acquiringTimeout := LOCKS_ACQUIRING_TIMEOUT

	if len(timeout) > 0 {
		acquiringTimeout = timeout[0]
	}

	// set locks tunnels cleanup timeout

	cleanupTimeout := TUNNELS_CLEANUP_TIMEOUT

	if len(timeout) > 1 {
		cleanupTimeout = timeout[1]
	}

	// build Provider

	tunnels := make(map[string]*LockTunnel)
	tunnelsCleanup := time.NewTicker(cleanupTimeout)

	tunnelsRequest := make(chan string)
	tunnelsResponse := make(chan LockTunnel)

	locksProvider := LockProvider{acquiringTimeout, tunnels, tunnelsCleanup, tunnelsRequest, tunnelsResponse}

	// run provider's locks managing thread
	go locksProvider.locksManager()

	return &locksProvider
}

func (this *LockProvider) selectChannels() []reflect.SelectCase {

	// Listen to "tunnels request" channel

	channels := make([]reflect.SelectCase, len(this.tunnels)+2)
	channels[TUNNELS_REQUEST_CHANNEL] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(this.tunnelsRequest)}

	// Listen to "tunnels cleanup" tick channel

	channels[TUNNELS_CLEANUP_CHANNEL] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(this.tunnelsCleanup.C)}

	// Listen to all delivered Locks releasing channels

	i := 2
	for _, tunnel := range this.tunnels {
		channels[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tunnel.current.release)}
		i++
	}

	return channels
}

func (this *LockProvider) locksManager() {
	for {
		chosen, value, ok := reflect.Select(this.selectChannels())

		if chosen == TUNNELS_REQUEST_CHANNEL {
			id := value.String()

			// first time a lock for this resource is requested? then build locks
			// tunnel where all routines will lock waiting for a Lock to get available

			if _, ok := this.tunnels[id]; !ok {
				lock, _ := NewLock(id)

				this.tunnels[id] = &LockTunnel{current: lock, channel: make(chan Lock, 1)}
				this.tunnels[id].channel <- lock
			}

			this.tunnels[id].pending++
			this.tunnelsResponse <- *this.tunnels[id]

		} else if chosen == TUNNELS_CLEANUP_CHANNEL {
			// Expired (no pending threads blocked in Lock()) tunnels memory cleanup, please
			// note that if you forget to Release() an acquired Lock its memory tunnels won't
			// be released, if you acquire always Locks with TTL this condition will never arise

			for key := range this.tunnels {
				if this.tunnels[key].pending == 0 {
					delete(this.tunnels, key)
				}
			}

		} else if ok {
			id := value.String()
			tunnel := this.tunnels[id]
			lock := tunnel.current

			// close "lock release channel" and decrease locks pending for release count

			lock.expiredMutex <- true

			close(lock.release)
			lock.expired = true

			<-lock.expiredMutex

			// if pending for locks, send new lock to locks feeding channel

			tunnel.pending--
			if tunnel.pending > 0 {
				lock, _ := NewLock(id)

				tunnel.current = lock
				tunnel.channel <- lock
			}
		}
	}
}

func autoReleaseLock(lock *Lock, ttl time.Duration) {
	autoRelease := time.NewTimer(ttl)
	autoReleaseCancel := make(chan bool)

	lock.releaseCancel = func() {
		autoReleaseCancel <- true
	}

	// run go routine which will release lock automatically, i'm using a go routine
	// instead of AfterFunc because i'm not able to extend the AfterFunc timeout once
	// set. Still, for what is my understanding, AfterFunc also launches a go routine

	go func() {
		select {
		case <-autoRelease.C:
			lock.Release()
			autoRelease.Stop()
		case <-autoReleaseCancel:
			// AutoRenew() called on Lock, client is now in charge of releasing it
		}
	}()
}

func (this *LockProvider) LockWithTTL(id string, ttl time.Duration) (*Lock, error) {
	return this.Lock(id, ttl)
}

func (this *LockProvider) Lock(id string, ttl ...time.Duration) (*Lock, error) {
	this.tunnelsRequest <- id
	tunnel := <-this.tunnelsResponse

	select {
	case lock := <-tunnel.channel:
		if len(ttl) > 0 {
			autoReleaseLock(&lock, ttl[0])
		}

		return &lock, nil
	case <-time.After(this.timeout):
		return nil, fmt.Errorf(LOCK_TIMEOUT_ERROR, id)
	}
}
