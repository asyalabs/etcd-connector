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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
)

// HEALTH-MONITOR
// This is just a wrapper API that can be used to monitor the status of the
// etcd cluster by querying the "/health" endpoint. The list of servers that
// make up the etcd service are queried to determine the status of the cluster.
//
// NOTE: The implementation is very much similar to the implementation of the
// "cluster-health" command of etcdctl tool provided by CoreOS. The source for
// that can be found here: github.com/coreos/etcd/etcdctl/command/cluster_health.go

// A type to indicate the status of the cluster.
type ClusterStatus int32

const (
	ClusterStatusHealthy ClusterStatus = iota
	ClusterStatusUnhealthy
	ClusterStatusUnknown
)

// A descriptor structure for the Health Monitor.
type HealthMonitor struct {
	// Pointer to the etcd connector descriptor.
	ec *EtcdConnector

	// Channel used to stop the monitor.
	stopCh chan bool

	// WaitGroup instance to make sure that the go-routine exits.
	wg *sync.WaitGroup
}

// Description:
//     A constructor routine to instantiate the HealthMonitor structure.
//
// Parameters:
//     None
//
// Return value:
//     1. A pointer to HealthMonitor structure.
func (ec *EtcdConnector) NewHealthMonitor() *HealthMonitor {
	return &HealthMonitor{
		ec:     ec,
		stopCh: make(chan bool),
		wg:     &sync.WaitGroup{},
	}
}

// Description:
//     A routine that starts the etcd cluster health monitor. The "/health" endpoint of
//     a client URL shall be probed to check the status of the cluster. The logic of this
//     routine is very similar to the implementation of "cluster-health" command of the
//     etcdctl tool provided by CoreOS. It's just that the functionality is being provided
//     in the form of an API. The caller gets a channel, as a return value, on which the
//     status will be posted.
//
// Parameters:
//     @once     - A flag to indicate to check the status once and exit.
//     @scanFreq - A time interval after which the status needs to be checked in a loop.
//                 This field makes sense when @once is set to false.
//
// Return value:
//     1. A channel on which ClusterStatus will be notified.
func (hm *HealthMonitor) Start(once bool, scanFreq time.Duration) <-chan ClusterStatus {
	statusCh := make(chan ClusterStatus)

	// Get an instance of an http client.
	hc := http.Client{
		Transport: client.DefaultTransport,
	}

	sentStatus := ClusterStatusUnknown

	// A closure method which checks the status of the cluster.
	checkHealth := func() {
		health := false

		// Loop through the list of servers that make up the etcd service and
		// try to access the "health" endpoint to get the status. If one fails
		// move on to the next URL in the list until all are tried.
		for _, url := range hm.ec.servers {
			resp, err := hc.Get(url + "/health")
			if err != nil {
				// Failed to check the health status via this cluster member.
				// Try the next URL.
				continue
			}

			res := struct{ Health string }{}
			nres := struct{ Health bool }{}
			bytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				// Couldn't read the response. Try the next URL.
				continue
			}
			resp.Body.Close()

			// Unmarshal the JSON object.
			err = json.Unmarshal(bytes, &res)
			if err != nil {
				err = json.Unmarshal(bytes, &nres)
				if err != nil {
					continue
				}
			}

			// Check if the health is ok.
			if res.Health == "true" || nres.Health == true {
				health = true
			}

			break
		}

		var s ClusterStatus
		if health == true {
			s = ClusterStatusHealthy
		} else {
			s = ClusterStatusUnhealthy
		}

		// Send the status only if the current status is different from the one
		// that was sent earlier.
		if s != sentStatus {
			statusCh <- s
			sentStatus = s
		}
	}

	// Account for the go-routine in the WaitGroup.
	hm.wg.Add(1)
	go func() {
		checkHealth()

		// If the user wants to perform a one shot check then exit.
		if once {
			close(statusCh)
			return
		}

		// Else start a ticker to periodically check the status.
		ticker := time.NewTicker(scanFreq)

		for {
			select {
			case <-ticker.C:
				checkHealth()
			case <-hm.stopCh:
				ticker.Stop()
				close(statusCh)
				hm.wg.Done()
				return
			}

		}
	}()

	return statusCh
}

// Description:
//     A routine that stops the health monitor. The stop channel will be written
//     to signal the go-routine to exit.
//
// Parameters:
//     None
//
// Return value:
//     None
func (hm *HealthMonitor) Stop() {
	hm.stopCh <- true
	hm.wg.Wait()
}
