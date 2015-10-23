// Copyright 2015 Asya Labs
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
//     its identity (IP address, host name etc...). Only one among all would
//     succeed in creating the key and that process would declare itself as the
//     leader. The leader will also start renewing the TTL of its key.
//   - All other processes will transition into a wait state wherein they start
//     observing the key. If the key gets either deleted or expired then all
//     the processes in the wait state attempt to become the leader.

// A descriptor structure for the leader election operation.
type LeaderElector struct {
	// Pointer to the etcd connection descriptor.
	ec *EtcdConnector

	// Key path that will be used to implement leader election.
	keyPath string

	// Value to be stored in the above key if the caller becomes leader.
	value string

	// An observer object to track the liveliness of the leader.
	obsvr *Observer

	// A time interval to be set as TTL value for @keyPath.
	ttl time.Duration

	// A time interval at which the TTL needs to be renewed.
	ttlRenew time.Duration

	// Context instance to handle cancellation.
	ctx context.Context

	// Function to call to initiate cancellation.
	cancel context.CancelFunc
}

// A type to describe the status of the leader election operation.
type LeaderElectorStatus int32

const (
	// Indicates that the caller has become the leader.
	LEStatusLeader LeaderElectorStatus = iota

	// Indicates that the caller has lost leaderhip.
	LEStatusFollower

	// Indicates that the state is unknown. Will be used when init fails.
	LEStatusUnknown
)

// A structure that will be sent back to the caller to notify if the caller
// has acquired or lost leadership. On success, @err will be nil.
type ElectionResponse struct {
	Status LeaderElectorStatus
	Err    error
}

const LE_SLEEP_INTERVAL = 5 * time.Second

// Description:
//     A constructor method used to instantiate the leader election operation.
//
// Parameters:
//     @key - key path that will be used as a lock for leader election.
//     @val - Value that will be stored in the key.
//
// Return value:
//     1. A pointer to a LeaderElector structure.
func (ec *EtcdConnector) NewLeaderElector(key, val string, interval time.Duration) *LeaderElector {
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

	return &LeaderElector{
		ec:       ec,
		keyPath:  key,
		value:    val,
		obsvr:    ec.NewObserver(key),
		ttl:      ttl,
		ttlRenew: ttlRenew,
		ctx:      nil,
		cancel:   nil,
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
func (le *LeaderElector) acquireLeadership() (*client.Response, error) {
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: TTL_VAL}
	return le.ec.Set(le.ctx, le.keyPath, le.value, opts)
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
func (le *LeaderElector) waitForLeadership(waitIndex uint64) (*client.Response, error) {
	var resp *client.Response = nil
	var err error = nil

	// Start the observer on @keyPath.
	oResp, err := le.obsvr.Start(waitIndex, false)
	if err != nil {
		return nil, err
	}

	for o := range oResp {
		// If any error is seen then stop the Observer and return back.
		if o.Err != nil {
			le.obsvr.Stop()
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
				le.obsvr.Stop()
				resp = o.Response
				break
			}
		}
	}

	return resp, err
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
// Return value:
//     1. A channel on which ElectionResponse will be notified.
func (le *LeaderElector) Start() <-chan ElectionResponse {
	var waitIndex uint64 = 0

	// Create a channel that carries ElectionResponse objects.
	out := make(chan ElectionResponse, 2)

	// The following channels are used to run the leader election
	// state machine.
	await := make(chan bool, 2)
	renew := make(chan bool, 2)

	// Setup a context with cancellation capability. This will be used to stop
	// the leader election operation.
	le.ctx, le.cancel = context.WithCancel(context.Background())
	if le.ctx == nil || le.cancel == nil {
		out <- ElectionResponse{
			Status: LEStatusUnknown,
			Err:    errors.New("Couldn't instantiate context/cancel objects"),
		}
		close(out)
		return out
	}

	go func() {
		for {
			select {
			case <-le.ctx.Done():
				// This means that the Stop on LeaderElector has been called.
				// So stop the Observer, close the outward channel ans return.
				le.obsvr.Stop()
				close(out)
				return
			case <-await:
				// Lost the race to get elected, so wait for leadership.
				resp, err := le.waitForLeadership(waitIndex)
				if err != nil {
					// If an error occurs during the wait informt the caller,
					// sleep for a while and attempt to acquire leadership again.
					out <- ElectionResponse{Status: LEStatusFollower, Err: err}
					time.Sleep(LE_SLEEP_INTERVAL)
				} else if resp != nil {
					// If woken up for the right reason then remember the
					// ModifiedIndex and attemp to acquire leadership again. The
					// ModifiedIndex can be used as the waitIndex if the attempt
					// fails.
					waitIndex = resp.Node.ModifiedIndex
				}
			case <-renew:
				// We get here if the attempt to acquire leadership was successful.
				// Now renew the TTL at regular intervals to retain the leadership
				// until the caller relinquishes leadership.
				errChan := le.ec.RenewTTL(le.ctx, le.keyPath, le.value, le.ttl, le.ttlRenew)
				for e := range errChan {
					// If any error occurred while renewing the TTl then it must
					// be catastrophic. Inform the caller about the issue, sleep
					// for a while and attempt to acquire leadership.
					out <- ElectionResponse{Status: LEStatusFollower, Err: e}
					time.Sleep(LE_SLEEP_INTERVAL)
					break
				}
			default:
				// By default an attempt will be made acquire leadership. Once
				// an attempt fails we fallback into WAIT state.
				_, err := le.acquireLeadership()
				if err != nil {
					errObj, ok := err.(client.Error)
					// If @keyPath is already present then someone else succeeded.
					// SO transition into WAIT state.
					if ok == true && errObj.Code == client.ErrorCodeNodeExist {
						await <- true
					} else {
						// An other error means that something catastrophic has
						// happened. So inform the caller, sleep for a while and
						// attempt to become leader again.
						out <- ElectionResponse{Status: LEStatusFollower, Err: err}
						time.Sleep(LE_SLEEP_INTERVAL)
					}
				} else {
					// If you acquire the leadership then transition into RENEW state.
					renew <- true

					// Inform the caller that the leadership has been acquired.
					out <- ElectionResponse{Status: LEStatusLeader, Err: nil}
				}
			}
		}
	}()

	return out
}

// Description:
//     A routine that stops the leader election operation.
//
// Parameters:
//     None
//
// Return value:
//     None
func (le *LeaderElector) Stop() {
	if le.cancel != nil {
		le.cancel()
	}
}
