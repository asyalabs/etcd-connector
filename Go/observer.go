package etcd_recipes

import (
	"errors"
	"strings"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

// OBSERVER RECIPE
//
// This recipe can mainly be used to get notifications when a key (or a directory)
// of interest undergoes change. This recipe is a wrapper around etcd's Watcher
// construct.
//
// If the entity being observed is a key then notifications are sent out when
// the key's value changes or the key gets deleted.
//
// If the entity being observed is a directory then notifications are sent out
// when a new child gets added/deleted under the directory or if the value of
// any of its children gets updated.

// A descriptor structure for the Observer operation.
type Observer struct {
	// Pointer to the etcd connection descriptor.
	ec *EtcdConnector

	// Key on which the watch needs to be placed.
	key string

	// Context instance to handle cancellation.
	ctx context.Context

	// Function to call to initiate the cancellation.
	cancel context.CancelFunc
}

// A wrapper structure that will be sent back to user as a response whenever
// a watch triggers or an error occurs.
type ObserverResponse struct {
	// An etcd response object received when a watch triggers.
	Response *client.Response

	// An error object, if any.
	err error
}

// Description:
//     A constructor method used to instantiate a new Observer structure.
//
// Parameters:
//     @key - Path to the key that needs to be observed. This could be a
//            directory or a key.
//
// Return value:
//     @obsvr - A pointer to an Observer structure.
func (ec *EtcdConnector) NewObserver(key string) *Observer {
	obsvr := &Observer{
		ec:     ec,
		key:    key,
		ctx:    nil,
		cancel: nil,
	}
	return obsvr
}

// Description:
//     A routine that starts the watch on the key identified by @key member
//     Observer structure. The caller gets a channel, as a return value, on
//     which ObserverResponse will be sent out.
//
// Parameters:
//     @waitIndex - The index of the key from which we intend to wait for changes.
//                  If waitIndex is zero that means that we wait for any change.
//     @recursive - A flag to indicate to look for changes recursively in a
//                  directory structure.
//
// Return values:
//     @resp - A channel object on which responses will be sent.
//     @err  - Error info, if any while starting the operation, will be returned.
func (o *Observer) Start(waitIndex uint64, recursive bool) (<-chan ObserverResponse, error) {
	// Create an outward channel on which responses will be sent.
	resp := make(chan ObserverResponse)

	// A WatcherOptions object specifying the AfterIndex and Recursive flags.
	opts := &client.WatcherOptions{AfterIndex: waitIndex, Recursive: recursive}

	// Instantiate an etcd Watcher object.
	watch := o.ec.Watcher(o.key, opts)
	if watch == nil {
		err := errors.New("Couldn't allocate etcd watcher instance")
		return nil, err
	}

	// Setup a context with cancellation capability. This will be used to stop
	// the watch.
	o.ctx, o.cancel = context.WithCancel(context.Background())
	if o.ctx == nil || o.cancel == nil {
		err := errors.New("Couldn't instantiate context/cancel objects")
		return nil, err
	}

	// Trigger the watch in a go routine.
	go func() {
		for {
			r, e := watch.Next(o.ctx)
			if e != nil {
				// On error, check if the context was cancelled. If so the caller
				// has asked to stop. Close the outward channel and return.
				if strings.Contains(e.Error(), "context canceled") {
					close(resp)
					return
				} else {
					// On any other error send the information back to caller. The
					// onus is on the caller to determine the action (can stop the
					// observation if the error is catastrophic).
					resp <- ObserverResponse{Response: nil, err: e}
				}
			} else {
				// If indeed there is any change detected, send the etcd Response
				// back to the caller.
				resp <- ObserverResponse{Response: r, err: nil}
			}
		}
	}()

	// Return the outward channel back to the caller.
	return resp, nil
}

// Description:
//     A routine that stops an Observer. The context with cancellation capability
//     will be used to stop the observation.
//
// Parameters:
//     None
//
// Return value:
//     None
func (o *Observer) Stop() {
	// Call cancel if it's setup.
	if o.cancel != nil {
		o.cancel()
	}
}
