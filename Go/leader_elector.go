// Copyright 2016 Asya Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd_recipes

import (
	"errors"
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

// LEADER-ELECTOR RECIPE
//
// This simple recipe can be used to elect a leader among a set of processes.
// The following is how the recipe operates:
//   - All the processes that participate in the elecion decide on a key path
//     to use for the election.
//   - As a first step every process tries to create the key with PrevExist
//     flag set to FALSE. The value that every process tries to set describes
//     its identity (IP address, port # etc...). Only one among all would
//     succeed in creating the key and that process would declare itself as the
//     leader. The leader will also start renewing the TTL of its key.
//   - All other processes will transition into a wait state wherein they start
//     observing the key. If the key gets either deleted or expired then all
//     the processes in the wait state attempt to become the leader.

// LeaderElector interface to be used by the users of the recipe.
type LeaderElector interface {
	// Start participating in the election process. A channel shall be returned
	// on which election status shall be reported back to the user.
	Start() (<-chan ElectionResponse, error)

	// Stop particpating in the election process.
	Stop()

	// Get the identity of the leader instance. This is a method via which
	// the followers can learn who the current leader is.
	GetLeader() string
}

// A type to describe the status of the leader election operation.
type LeaderElectorStatus int32

const (
	// Indicates that the caller has become the leader.
	LEStatusLeader LeaderElectorStatus = iota

	// Indicates that the caller has lost leaderhip.
	LEStatusFollower
)

// A structure that will be sent back to the caller to notify if the caller
// has acquired or lost leadership. On success, @err will be nil.
type ElectionResponse struct {
	Status LeaderElectorStatus
	Err    error
}

// A descriptor structure for the particpiant in the election.
type participant struct {
	ec            *EtcdConnector     // Pointer to the etcd connector descriptor.
	keyPath       string             // Key path that will be used to implement leader election.
	value         string             // Value to be stored in the above key if the caller becomes leader.
	currentLeader string             // Value of the instance that is currently the leader.
	lock          *sync.Mutex        // Lock to protect access to fields.
	obsvr         *Observer          // An observer object to track the liveliness of the leader.
	ttl           time.Duration      // A time interval to be set as TTL value for @keyPath.
	ttlRenew      time.Duration      // A time interval at which the TTL needs to be renewed.
	ctx           context.Context    // Context instance to handle cancellation.
	cancel        context.CancelFunc // Function to call to initiate cancellation.
	wg            *sync.WaitGroup    // WaitGroup instance used to wait for the go-routine to exit.
}

const LE_SLEEP_INTERVAL = 5 * time.Second

// Description:
//     A constructor method used to instantiate a leader election participant.
//
// Parameters:
//     @key - key path that will be used as a lock for leader election.
//     @val - Value that will be stored in the key.
//
// NOTE: @val is the value that will be returned back when a follower
//       calls the "GetLeader" method. So the user will have to choose
//       the value in a way that will be easy for the followers to
//       identify the leader instance. (Example value : <IP addr:Port>)
//
// Return value:
//     1. A LeaderElector interface.
func (ec *EtcdConnector) NewLeaderElector(key, val string, interval time.Duration) LeaderElector {
	var ttl, ttlRenew time.Duration

	if interval == 0 {
		ttl = TTL_VAL
		ttlRenew = TTL_REFRESH_TIMEOUT
	} else {
		ttl = interval

		// If the interval specified is less than the RTT required to reach the
		// etcd servers then set ttl to RTT + interval and the ttl refresh time
		// to the original interval passed in.
		if ttl <= ec.serverRTT {
			ttlRenew = ttl
			ttl += ec.serverRTT
		} else {
			// Else set the ttl refresh time to ttl - RTT.
			ttlRenew = ttl - ec.serverRTT
		}
	}

	return &participant{
		ec:            ec,
		keyPath:       key,
		value:         val,
		currentLeader: "",
		lock:          &sync.Mutex{},
		obsvr:         ec.NewObserver(key),
		ttl:           ttl,
		ttlRenew:      ttlRenew,
		ctx:           nil,
		cancel:        nil,
		wg:            &sync.WaitGroup{},
	}
}

// Description:
//     A routine that attempts to acquire leadership by trying to create
//     @keyPath with PrevExist flag set to false.
//
// Parameters:
//     None
//
// Return values:
//     1. An etcd Response structure on success, nil otherwise.
//     2. An error object on failure, nil otherwise.
//func (le *LeaderElector) acquireLeadership() (*client.Response, error) {
func (p *participant) acquireLeadership() (*client.Response, error) {
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: TTL_VAL}
	return p.ec.Set(p.ctx, p.keyPath, p.value, opts)
}

// Description:
//     A routine that waits to acquire leadership. We would get here only when
//     the attempt to acquire leadership fails. This routine uses an Observer to
//     lookout for changes to @keyPath. If the Observer indicates that @keyPath
//     got "set" or "update" then the routine continues to wait. But if the
//     Observer indicates that @keyPath got "expired" or "delete" then the
//     routine stops the Observer and returns back to caller.
//
// Parameters:
//     @waitIndex - The etcd index after which the changes will be monitored.
//
// Return value:
//     1. An etcd Response structure on success, nil otherwise.
//     2. An error object on failure, nil otherwise.
func (p *participant) waitForLeadership(waitIndex uint64) (*client.Response, error) {
	var resp *client.Response = nil
	var err error = nil

	// Start the observer on @keyPath.
	oResp, err := p.obsvr.Start(waitIndex, false)
	if err != nil {
		return nil, err
	}

	for o := range oResp {
		// If any error is seen then stop the Observer and return back.
		if o.Err != nil {
			p.obsvr.Stop()
			err = o.Err
			break
		} else {
			// If the response is "update" or "set", continue with the observation.
			if o.Response.Action == "update" || o.Response.Action == "set" {
				waitIndex = o.Response.Node.ModifiedIndex
				continue
			} else if o.Response.Action == "expire" || o.Response.Action == "delete" {
				// If the response is "expired" or "delete" then stop the Observer
				// and return back to the caller so that the caller can attempt to
				// become a leader.
				p.obsvr.Stop()
				resp = o.Response
				break
			}
		}
	}

	return resp, err
}

// Description:
//     A helper routine to renew the TTL of @keyPath. This routine starts a
//     timer and at every tick attempts to renew the TTL of the key.
//
// Parameters:
//     None
//
// Return value:
//     1. An error object in case of renewal failure, nil otherwise.
func (p *participant) renewTTL() error {
	// Start a ticker to trigger at every @ttlRenew seconds.
	ticker := time.NewTicker(p.ttlRenew)
	defer ticker.Stop()

	opts := &client.SetOptions{PrevExist: client.PrevExist, TTL: p.ttl}

	// Wait for activity either on the ticker channel or @ctx's channel.
	for {
		select {
		case <-ticker.C:
			// Update the TTL of @keyPath.
			_, err := p.ec.Set(p.ctx, p.keyPath, p.value, opts)
			if err != nil {
				return err
			}
		case <-p.ctx.Done():
			// If we are here then it's indication by the caller to stop the
			// renewal operation. So delete @keyPath and return.

			// Deletion is best effort here and also an optimization. We end
			// up here only when the program is being shutdown or when a
			// catastrophic failure occurs. If we succeed in deleting @keyPath
			// then entities watching @keyPath will get notified quickly or else
			// they'll have to wait for the TTL to expire. But if we get here
			// because of catastrophic failure then it's a best effort as the
			// delete can also fail.
			p.ec.Delete(context.Background(), p.keyPath, &client.DeleteOptions{})
			return nil
		}
	}
}

// Description:
//     A helper routine to update the "currentLeader" field. This is done holding
//     the mutex.
//
// Parameters:
//     @val - A string descrbing the identity of the leader. This is essentially
//            the string passed in as @val to NewLeaderElector method.
//
// Return value:
//     None
func (p *participant) updateCurrentLeader(val string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.currentLeader = val
}

// Description:
//     A helper routine to setup the cancel function on a context object. This
//     is done atomically by holding the mutex. The reason for this is that the
//     fact that a cancel function is set will be used to determine if the
//     instance is already participating in the election.
//
// Parameters:
//     None
//
// Return value:
//     1. An error object in case of failure, nil otherwise.
func (p *participant) setupCancelFunc() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.cancel != nil {
		return errors.New("Already participating in the election")
	}

	// Setup a context with cancellation capability. This will be used to stop
	// the leader election operation.
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if p.ctx == nil || p.cancel == nil {
		return errors.New("Couldn't instantiate context/cancel objects")
	}

	return nil
}

// Description:
//     A routine to start the leader election. As a first step the routine tries
//     to become the leader by creating @keyPath atomically. If create operation
//     succeeds then the routine transitions itself into RENEW state where it
//     periodically renews the TTL value of @keyPath. If crete operation fails,
//     then the routine transitions into WAIT state where it'll start an
//     Observer on @keyPath. If @keyPath gets deleted or expired then the process
//     is started from the first step.
//
// Parameters:
//     None
//
// Return value:
//     1. A channel on which ElectionResponse will be notified.
//     2. Error object describing the error, if any, that occurs during initial setup.
func (p *participant) Start() (<-chan ElectionResponse, error) {
	var waitIndex uint64 = 0

	// Create a channel that carries ElectionResponse objects.
	out := make(chan ElectionResponse)

	// The following buffered channels are used to run the leader election
	// state machine.
	await := make(chan bool, 2)
	renew := make(chan bool, 2)

	err := p.setupCancelFunc()
	if err != nil {
		close(out)
		return nil, err
	}

	// Account for the go-routine in WaitGroup.
	p.wg.Add(1)

	go func() {
		for {
			select {
			case <-p.ctx.Done():
				// This means that the Stop on LeaderElector has been called.
				// So stop the Observer.
				p.obsvr.Stop()

				// Close the outward channel and set the channel to nil.
				close(out)
				out = nil

				// Adjust the WaitGroup counter before returning.
				p.wg.Done()
				return
			case <-await:
				// Read the key to record who the current leader is.
				resp, err := p.ec.Get(p.ctx, p.keyPath, &client.GetOptions{})
				if err != nil {
					errObj, ok := err.(client.Error)
					// If the key is not found then it means that the current
					// leader has died. So attempt to become the leader.
					if ok == true && errObj.Code == client.ErrorCodeKeyNotFound {
						continue
					} else {
						out <- ElectionResponse{Status: LEStatusFollower, Err: err}
						time.Sleep(LE_SLEEP_INTERVAL)
					}
				} else {
					// If we are here then another participant has acquired
					// leadership. So update the waitIndex, make note of the
					// identity of the current leader and inform the caller
					// about the same.
					waitIndex = resp.Node.ModifiedIndex
					p.updateCurrentLeader(resp.Node.Value)
					out <- ElectionResponse{Status: LEStatusFollower, Err: nil}
				}

				// Lost the race to get elected, so wait for leadership.
				resp, err = p.waitForLeadership(waitIndex)
				if err != nil {
					// If an error occurs during the wait inform the caller,
					// sleep for a while and attempt to acquire leadership again.
					out <- ElectionResponse{Status: LEStatusFollower, Err: err}
					time.Sleep(LE_SLEEP_INTERVAL)
				} else if resp != nil {
					// If woken up for the right reason then attempt to acquire
					// leadership again.
					continue
				}
			case <-renew:
				// We get here if the attempt to acquire leadership was successful.
				// Now renew the TTL at regular intervals to retain the leadership
				// until the caller relinquishes leadership.
				err = p.renewTTL()
				if err != nil {
					// If any error occurred while renewing the TTL then it must
					// be catastrophic. Reset the current leader info, inform the
					// caller about the issue, sleep for a while and attempt to
					// acquire leadership again.
					p.updateCurrentLeader("")
					out <- ElectionResponse{Status: LEStatusFollower, Err: err}
					time.Sleep(LE_SLEEP_INTERVAL)
				}
			default:
				// By default an attempt will be made acquire leadership. Once
				// an attempt fails we fallback into WAIT state.
				_, err = p.acquireLeadership()
				if err != nil {
					errObj, ok := err.(client.Error)
					// If @keyPath is already present then someone else succeeded.
					// So transition into WAIT state.
					if ok == true && errObj.Code == client.ErrorCodeNodeExist {
						await <- true
					} else {
						// Another error means that something catastrophic has
						// happened. So inform the caller, sleep for a while and
						// attempt to become leader again.
						out <- ElectionResponse{Status: LEStatusFollower, Err: err}
						time.Sleep(LE_SLEEP_INTERVAL)
					}
				} else {
					// If you acquire the leadership then update the respective
					// field to indicate the same and transition into RENEW state.
					renew <- true
					p.updateCurrentLeader(p.value)

					// Inform the caller that the leadership has been acquired.
					out <- ElectionResponse{Status: LEStatusLeader, Err: nil}
				}
			}
		}
	}()

	return out, nil
}

// Description:
//     A routine that stops the user from participating in the leader election
//     operation.
//
// Parameters:
//     None
//
// Return value:
//     None
func (p *participant) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.wg.Wait()
		p.cancel = nil
		p.ctx = nil
	}
}

// Description:
//     A routine that returns the identity of the current leader. This is done
//     so by holding the mutex. If a leader is not elected yet then "" will be
//     returned.
//
// Parameters:
//     None
//
// Return value:
//     1. A string representing the identity of the leader. "" if none is elected.
func (p *participant) GetLeader() string {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.currentLeader
}
