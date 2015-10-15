package etcd_recipes

import (
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

// SERVICE-TRACKER RECIPE
//
// This recipe can be used to discover the instances that make up a distributed
// service. The instances can be running anywhere in the cluster. The following
// is how it works:
//   - All the instances that make up a distributed service create an ephemeral
//     key under a well known directory in the etcd namespace. The ephemeral keys
//     provide the liveness tracking capability. So, if an instance dies then its
//     corresponding key also disappears. The value stored in the ephemral key
//     can provide details of the instance like IP address/port number etc...
//   - The interested parties will instantiate a ServiceTracker recipe which
//     observes the well known path to identify the instances that make up the
//     service in question. The ServiceTracker recipe makes use of the Observer
//     recipe to track changes.
//   - The ServiceTracker notifies the caller of any change in the directory by
//     sending a list of all the instances that are present under the directory.

// A descriptor structure for the service tracking operation.
type ServiceTracker struct {
	// Pointer to the etcd connection descriptor.
	ec *EtcdConnector

	// Path under which the service instances will be tracked.
	servicePath string

	// Observer instance to monitor the changes.
	obsvr *Observer
}

// A structure that describes a key-value pair.
type Pair struct {
	Key   string
	Value string
}

// A structure that will be sent back to the caller whenever a change
// is observed under @servicePath.
type TrackerData struct {
	// An array of all active service instances represented as key-value pairs.
	Pairs []Pair

	// Error information, if any.
	err error
}

// Description:
//     A constructor routine to instantiate a service tracking operation.
//
// Parameters:
//     @path - A path in the etcd namespace under which the instances will
//             be tracked.
//
// Return value:
//     @st - A pointer to the ServiceTracker instance.
func (ec *EtcdConnector) NewServiceTracker(path string) *ServiceTracker {
	st := &ServiceTracker{
		ec:          ec,
		servicePath: path,
		obsvr:       ec.NewObserver(path),
	}
	return st
}

// Description:
//     A routine to start the service tracking operation. This routine starts
//     an Observer on @servicePath and waits to hear from the Observer about
//     changes. If any change is seen then it compares the current array of
//     Pairs with a cached version. If any difference is seen at all then it
//     posts the current Pairs on the outward channel.
//
// Parameters:
//     None
//
// Return value:
//
func (st *ServiceTracker) Start() <-chan TrackerData {
	// Create an outward channel on which service tracker info will be sent.
	tracker := make(chan TrackerData, 2)

	// Start the Observer.
	obResp, err := st.obsvr.Start(0, true)
	if err != nil {
		tracker <- TrackerData{Pairs: nil, err: err}
		close(tracker)
		return tracker
	}

	var curKeyVals []Pair
	opts := &client.GetOptions{Sort: true, Recursive: true}

	// Observe the changes in a go routine.
	go func() {
		for or := range obResp {
			// For every trigger check if anything has changed.
			updated := false

			// If any error, report it back to the caller. Rely on the caller
			// to handle the error appropriately.
			if or.err != nil {
				tracker <- TrackerData{Pairs: nil, err: or.err}
				continue
			}

			var newKeyVals []Pair

			// Get the latest contents of @servicePath directory.
			r, e := st.ec.Get(context.Background(), st.servicePath, opts)
			if e != nil {
				tracker <- TrackerData{Pairs: nil, err: e}
				continue
			}

			// Construct the new key-value pairs found under @servicePath
			for i := 0; i < len(r.Node.Nodes); i++ {
				newKeyVals = append(newKeyVals, Pair{
					Key:   r.Node.Nodes[i].Key,
					Value: r.Node.Nodes[i].Value,
				})
			}

			// Perform a diff.
			if curKeyVals == nil || len(curKeyVals) != len(newKeyVals) {
				curKeyVals = newKeyVals
				updated = true
			} else {
				for i := 0; i < len(newKeyVals); i++ {
					if curKeyVals[i].Key != newKeyVals[i].Key ||
						curKeyVals[i].Value != newKeyVals[i].Value {
						curKeyVals = newKeyVals
						updated = true
						break
					}
				}
			}

			// if anything has changed then send the new pairs to the caller.
			if updated == true {
				tracker <- TrackerData{Pairs: curKeyVals, err: nil}
			}
		}

		// If the observer channel is closed then close the tracker channel too.
		close(tracker)
	}()

	return tracker
}

// Description:
//     A routine to stop the service tracking openation.
//
// Parameters:
//     None
//
// Return value:
//     None
func (st *ServiceTracker) Stop() {
	st.obsvr.Stop()
}
