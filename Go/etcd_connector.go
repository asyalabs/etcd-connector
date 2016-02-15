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
	"fmt"
	"time"

	"github.com/coreos/etcd/client"
)

// Etcd is a distributed key-value store which can be used for the following
// purposes:
//     [1] To store cluster wide configuration information. Hence the
//         name "EtcdConnector".
//     [2] To achieve distributed coordination.
//
// This structure acts as a handle to the etcd service. The handle can be used
// to perform all the client operations (like set, get, watch etc...) on the
// etcd service. It can also be used to make use of some common recipes like
// leader election, service discovery etc... on the etcd service.
type EtcdConnector struct {
	// etcd client API Interface. This is used as an anonymous field.
	client.KeysAPI

	// etcd client configuration structure.
	cfg client.Config

	// etcd client instance.
	cli client.Client

	// List of servers running the etcd service.
	servers []string

	// Namespace identifier. All keys will be created under this directory.
	name string

	// RTT required to reach the Etcd servers.
	serverRTT time.Duration
}

const (
	// Timeout value set on every http request sent to the etcd service.
	TIMEOUT_PER_REQUEST = (2 * time.Second)

	// Default TTL value that will be used for keys (used mainly by
	// EphemeralKey and LeaderElector operations).
	TTL_VAL = (10 * time.Second)

	// Default ticker value that will be used to refresh the TTL.
	TTL_REFRESH_TIMEOUT = (TTL_VAL - (1 * time.Second))
)

// Description:
//     A constructor routine to instantiate a connection to the etcd service.
//
// Parameters:
//     @servers - Array of server host:port pairs to which a client connection
//                will be attempted.
//     @name    - A string that identifies the namespace under which all the
//                keys will be created.
//                For Ex: If @name is set to 'abc' and a key named 'hosts' is
//                created then 'hosts' would be present at '/abc/hosts'.
//
// Return values:
//     1. A pointer to an EtcdConnector structure on success, nil otherwise.
//     2. An error object on failure, nil otherise.
func NewEtcdConnector(servers []string, name string) (*EtcdConnector, error) {
	var err error
	ec := &EtcdConnector{
		cfg: client.Config{
			Endpoints:               servers,
			Transport:               client.DefaultTransport,
			HeaderTimeoutPerRequest: TIMEOUT_PER_REQUEST,
		},
		servers: servers,
		name:    "/" + name + "/",
	}

	// Get an instance of the etcd.Client structure
	ec.cli, err = client.New(ec.cfg)
	if err != nil {
		return nil, err
	}

	// Get the KeysAPI interface
	ec.KeysAPI = client.NewKeysAPI(ec.cli)

	return ec, nil
}

// Description:
//     A helper routine to construct an array of strings from the given server
//     list and port number into the following format 'http://<server>:<port>'.
//
// Parameters:
//     @srvs - An array of strings representing the hostname or IP addresses of
//             all the servers that make up the etcd service.
//     @port - Port at which the servers will accept client connections.
//
// Return value:
//     1. An array of strings in 'http://<server>:<port>' format.
func PrepareEndpointList(srvs []string, port int) []string {
	if len(srvs) == 0 {
		return nil
	}

	epList := make([]string, len(srvs))
	for _, s := range srvs {
		epList = append(epList, fmt.Sprintf("http://%s:%d", s, port))
	}

	return epList
}

// Description:
//     A helper routine to construct a path under the namespace identified by
//     @name field in EtcdConnector structure.
//
// Parameters:
//     @dir - A directory (already existing) under which @key will be created.
//     @key - A key to be created.
//
// Return value:
//     1. A string that takes following form: /@EtcdConnector.name/@dir/@key
func (ec *EtcdConnector) ConstructPath(dir string, key string) string {
	path := ec.name + dir + "/" + key
	return path
}

// Description:
//     A helper routine to approximate the time required for a request to reach
//     one of the etcd servers. This needs to be accounted as it will be crucial
//     for picking up a time interval to update the TTL of a key.
//
//     At the end of this routine the approximate RTT value will be stored in the
//     EtcdConnector structure.
//
// Parameters:
//     None
//
// Return value:
//     1. Corresponding error object, if any.
func (ec *EtcdConnector) ComputeServerRTT() error {
	// TODO - use the ping times to determine the RTT.
	return nil
}
