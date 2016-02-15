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

import "sync"

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
//     service in question. ServiceTracker makes use of the Observer recipe to
//     track changes.
//   - The ServiceTracker notifies the caller of any change in the directory by
//     sending a map of all the instances that are present under the directory.

// ServiceTracker interface to be used by the users of this recipe.
type ServiceTracker interface {
	// Start tracking the instances that form a given service. A channel shall
	// be returned on which the details about the service instances will be sent.
	Start() (<-chan ServiceData, error)

	// Stop tracking for changes.
	Stop()
}

// A structure that will be sent back to the caller whenever a change
// is observed under @servicePath.
type ServiceData struct {
	// A map of active services with service name as key and its content
	// as value.
	Services map[string]string

	// Error information, if any.
	Err error
}

// A descriptor structure for the service tracking operation.
type tracker struct {
	// Pointer to the etcd connection descriptor.
	ec *EtcdConnector

	// Path under which the service instances will be tracked.
	servicePath string

	// Observer instance to monitor the changes.
	obsvr *Observer

	// WaitGroup instance used to wait for the go-routine to exit.
	wg *sync.WaitGroup
}

// Description:
//     A constructor routine to instantiate a service tracker.
//
// Parameters:
//     @path - A path in the etcd namespace under which the instances will
//             be tracked.
//
// Return value:
//     1. A pointer to the ServiceTracker instance.
func (ec *EtcdConnector) NewServiceTracker(path string) ServiceTracker {
	t := &tracker{
		ec:          ec,
		servicePath: path,
		obsvr:       ec.NewObserver(path),
		wg:          &sync.WaitGroup{},
	}
	return t
}

// Description:
//     A routine to start the service tracking operation. This routine starts
//     an Observer on @servicePath and waits to hear from the Observer about
//     changes in the form of etcd Response object. Based on the change described
//     by the "Action" field in the Response object the map of services will be
//     updated appropriately and caller will be notified about the change via
//     the outward channel.
//
// Parameters:
//     None
//
// Return value:
//     1. A channel on which ServiceData will be notified.
//     2. An error object describing any errors seen during instantiation.
func (t *tracker) Start() (<-chan ServiceData, error) {
	// Create an outward buffered channel on which service instance changes
	// will be notified to the caller.
	tracker := make(chan ServiceData, 2)

	// Start the Observer.
	obResp, err := t.obsvr.Start(0, true)
	if err != nil {
		close(tracker)
		return nil, err
	}

	// Account for the go-routine in WaitGroup.
	t.wg.Add(1)

	// Observe the changes in a go routine.
	go func() {
		services := make(map[string]string)
		for or := range obResp {
			// For every trigger check if anything has changed.
			updated := false

			// If any error, report it back to the caller. Rely on the caller
			// to handle the error appropriately.
			if or.Err != nil {
				tracker <- ServiceData{Services: nil, Err: or.Err}
				continue
			}

			resp := or.Response

			if resp.Action == "create" {
				// If the Action is "create" then a new service instance has
				// appeared under @servicePath. So, add the new entry to the
				// @services map.
				services[resp.Node.Key] = resp.Node.Value
				updated = true
			} else if resp.Action == "update" {
				// If the Action is "update" then see if the contents of the
				// service instance has changed. If so, update the local services
				// map. The check is needed as the current etcd version wakes
				// up the watcher even if the TTL on a key is updated.
				if services[resp.Node.Key] != resp.Node.Value {
					services[resp.Node.Key] = resp.Node.Value
					updated = true
				}
			} else if resp.Action == "delete" || resp.Action == "expire" {
				// If the Action is either "delete" or "expire" delete the entry
				// from the local services map. This indicates that the service
				// instance has exited or died.
				delete(services, resp.Node.Key)
				updated = true
			}

			// If anything has changed then send the new pairs to the caller.
			if updated == true {
				tracker <- ServiceData{Services: services, Err: nil}
			}
		}

		// If the observer channel is closed then close the tracker channel too.
		close(tracker)
		tracker = nil

		// Adjust the WaitGroup counter before exiting the go-routine.
		t.wg.Done()
	}()

	return tracker, nil
}

// Description:
//     A routine to stop the service tracking operation.
//
// Parameters:
//     None
//
// Return value:
//     None
func (t *tracker) Stop() {
	t.obsvr.Stop()
	t.wg.Wait()
}
