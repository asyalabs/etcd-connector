package etcd_recipes

import (
	"errors"
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
}

// Description:
//     A constructor routine to instantiate an EphemeralKey structure.
//
// Parameters:
//     @path - Path in etcd namespace where the ephemeral key will be created.
func (ec *EtcdConnector) NewEphemeralKey(path string) *EphemeralKey {
	ek := &EphemeralKey{
		ec:      ec,
		keyPath: path,
		ticker:  nil,
	}
	return ek
}

// Description:
//     A routine that announces the existence of an ephemeral key by creating
//     one at @keyPath specified. The routine also sets @val, passed by the
//     user, as the value to the key and uses @interval as the time to renew
//     the TTL of the key at regular intervals.
func (se *EphemeralKey) Announce(val string, interval time.Duration) <-chan error {
	var ttl time.Duration
	var ttlRefresh time.Duration

	// Create an error channel on which errors will be reported to the user.
	errCh := make(chan error, 2)

	// Setup a context with cancellation capability. This will be used to stop
	// the EphemeralKey instance.
	se.ctx, se.cancel = context.WithCancel(context.Background())
	if se.ctx == nil || se.cancel == nil {
		errCh <- errors.New("Couldn't instantiate context/cancel objects")
		close(errCh)
		return errCh
	}

	// If @interval is not passed in then default TTL and TTL_REFRESH values
	// will be picked.
	if interval == 0 {
		ttl = TTL_VAL
		ttlRefresh = TTL_REFRESH_TIMEOUT
	} else {
		ttl = interval
		// If the interval is specified then pick a ttlRefresh interval that
		// accounts the approximate worst case time needed for the HTTP request
		// to reach the etcd cluster.
		ttlRefresh = interval - (2 * time.Millisecond)
	}

	// Since we are announcing the presence of the ephemeral key for the first
	// time set the PrevExist flag to false.
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl}
	_, err := se.ec.Set(se.ctx, se.keyPath, val, opts)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	// Call the helper routine to periodically renew the TTL of the key.
	return se.ec.RenewTTL(se.ctx, se.keyPath, val, ttl, ttlRefresh)
}

// Description:
//     A routine to stops the EphemeralKey instance. Once stopped an attempt
//     will be made to manually delete the ephemeral key from etcd namespace.
//     The deletion is handled by the RenewTTL helper routine.
//
// Parameters:
//     None
//
// Return value:
//     None
func (se *EphemeralKey) Stop() {
	// Call cancel if it's setup.
	if se.cancel != nil {
		se.cancel()
	}
}
