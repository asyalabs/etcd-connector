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
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

// EPHEMERAL-KEY RECIPE
//
// This receipe can mainly be used if there is a need for liveness tracking.
// This recipe essentially allows a host to create a key in the etcd namespace
// and periodically renew the TTL on the key. If the host ever dies then the TTL
// will eventually expire and the corresponding key gets dropped. If entity
// interested in liveness tracking has placed a watch on the ephemeral key then
// it would be notified when the ephemeral key gets dropped.

// A descriptor structure for the Ephemeral key.
type EphemeralKey struct {
	// Pointer to the etcd connection descriptor.
	ec *EtcdConnector

	// Path at which the ephemeral key will be created.
	keyPath string

	// A ticker object to periodically renew the TTL.
	ticker *time.Ticker

	// Context instance to handle cancellation.
	ctx context.Context

	// Function to call to initiate the cancellation.
	cancel context.CancelFunc

	// TTL value.
	ttl time.Duration

	// TTL Refresh value.
	ttlRefresh time.Duration

	// WaitGroup to make sure that the go-routine has really exited.
	wg *sync.WaitGroup
}

// Description:
//     A constructor routine to instantiate an EphemeralKey structure.
//
// Parameters:
//     @path - Path in etcd namespace where the ephemeral key will be created.
//
// Return value:
//     1. A pointer to EphemeralKey structure.
func (ec *EtcdConnector) NewEphemeralKey(path string) *EphemeralKey {
	ek := &EphemeralKey{
		ec:      ec,
		keyPath: path,
		ticker:  nil,
		wg:      &sync.WaitGroup{},
	}
	return ek
}

// Description:
//     A routine that creates the existence of an ephemeral key by creating
//     one at @keyPath specified. The routine also sets @val, passed by the
//     user, as the value to the key and uses @interval as the time to renew
//     the TTL of the key at regular intervals.
//
// Parameters:
//     @val      - User defined value that will be set for the ephemeral key.
//     @interval - Interval at which the TTL will be renewed.
//
// Return value:
//     1. A channel on which errors that occur during TTL renewal will be notified.
//     2. Error object describing the error, if any, that occurs during initial setup.
func (ek *EphemeralKey) Create(val string, interval time.Duration) (<-chan error, error) {
	// Setup a context with cancellation capability. This will be used to stop
	// the EphemeralKey instance.
	ek.ctx, ek.cancel = context.WithCancel(context.Background())
	if ek.ctx == nil || ek.cancel == nil {
		return nil, errors.New("Couldn't instantiate context/cancel objects")
	}

	//TODO: Currently, interval will be set to 0 to always pick the default
	//      TTL values and will be removed once the "ComputeServerRTT" is done.
	interval = 0

	// If @interval is not passed in then default TTL and TTL_REFRESH values
	// will be picked.
	if interval == 0 {
		ek.ttl = TTL_VAL
		ek.ttlRefresh = TTL_REFRESH_TIMEOUT
	} else {
		ek.ttl = interval

		// If the interval specified is less than the RTT required to reach the
		// etcd servers then set ttl to RTT + interval and the ttl refresh time
		// to the original interval passed in.
		if ek.ttl <= ek.ec.serverRTT {
			ek.ttlRefresh = ek.ttl
			ek.ttl += ek.ec.serverRTT
		} else {
			// Else set the ttl refresh time to ttl - RTT.
			ek.ttlRefresh = ek.ttl - ek.ec.serverRTT
		}
	}

	// Since we are announcing the presence of the ephemeral key for the first
	// time set the PrevExist flag to false.
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: ek.ttl}
	_, err := ek.ec.Set(ek.ctx, ek.keyPath, val, opts)
	if err != nil {
		return nil, err
	}

	// Call the helper routine to periodically renew the TTL of the key.
	errCh := ek.ec.RenewTTL(ek.ctx, ek.wg, ek.keyPath, val, ek.ttl, ek.ttlRefresh)
	return errCh, nil
}

// Description:
//     A routine that updates the value stored in the ephemeral key. It also
//     sets the TTL to an already determined value (in the call to Create).
//
// Parameters:
//     @newVal - A new user defined value that will be set for the ephemeral key.
//
// Return value:
//     1. Error object describing the error.
func (ek *EphemeralKey) Update(newVal string) error {
	// Make sure that the ephemral key is already created.
	if ek.cancel == nil {
		return errors.New("Update cannot be called before Create!")
	}

	// Set the options with PrevExist and @ek.ttl value.
	opts := &client.SetOptions{PrevExist: client.PrevExist, TTL: ek.ttl}

	// Update the ephemeral key's value.
	_, err := ek.ec.Set(context.Background(), ek.keyPath, newVal, opts)
	return err
}

// Description:
//     A routine that deletes the EphemeralKey instance. Once stopped an attempt
//     will be made to manually delete the ephemeral key from etcd namespace.
//     The deletion is handled by the RenewTTL helper routine.
//
// Parameters:
//     None
//
// Return value:
//     None
func (ek *EphemeralKey) Delete() {
	// Call cancel if it's setup.
	if ek.cancel != nil {
		ek.cancel()
		ek.wg.Wait()
	}
}
