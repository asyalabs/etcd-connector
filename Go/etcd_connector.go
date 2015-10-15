package etcd_recipes

import (
	"fmt"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
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
}

const (
	// Timeout value set on every http request sent to the etcd service.
	TIMEOUT_PER_REQUEST = (2 * time.Second)

	// Default TTL value that will be used for keys (used mainly by
	// EphemeralKey and LeaderElector operations).
	TTL_VAL = (10 * time.Second)

	// Default ticker value that will be used to refresh the TTL.
	TTL_REFRESH_TIMEOUT = (TTL_VAL - (2 * time.Millisecond))
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
//   On Success,
//     @ec  - A pointer to an instance of the EtcdConnector structure.
//     @err - nil.
//
//   On failure,
//     @ec  - nil.
//     @err - Error object describing the error.
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
//     @epList - an array of strings in 'http://<server>:<port>' format.
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
//     @path - A string that takes following form: /@EtcdConnector.name/@dir/@key
func (ec *EtcdConnector) ConstructPath(dir string, key string) string {
	path := ec.name + dir + "/" + key
	return path
}

// Description:
//     A helper routine to renew the TTL of o given key. This routine starts a
//     timer and at every tick attempts to renew the TTL of the given key.
//
// Parameters:
//     @ctx     - A context object used to stop the renewal operation.
//     @key     - The key for which the TTL needs to be renewed.
//     @val     - The value associated with the key.
//     @ttl     - TTL value in seconds.
//     @refresh - Interval, in seconds, at which the TTL will be renewed.
//
// Return value:
//     @out - A channel on which errors will be reported back to the caller.
func (ec *EtcdConnector) RenewTTL(ctx context.Context, key, val string, ttl, refresh time.Duration) <-chan error {
	out := make(chan error)

	// Start a ticker to trigger at every @refresh seconds.
	ticker := time.NewTicker(refresh)

	// Launch a go routine to handle the periodic TTL renewal of @key.
	go func() {
		opts := &client.SetOptions{PrevExist: client.PrevExist, TTL: ttl}

		// Wait for activity either on the ticker channel or @ctx's channel.
		for {
			select {
			case <-ticker.C:
				// Update the TTL of @key.
				_, err := ec.Set(ctx, key, val, opts)
				if err != nil {
					// In case of an error, put out the error info on the
					// outward channel and return.
					out <- err
					close(out)
					return
				}
			case <-ctx.Done():
				// If we are here then it's indication by the caller to stop the
				// renewal operation. So kill the ticker, delete @key, close the
				// outward channel and return.
				ticker.Stop()

				// Deletion is best effort here and also an optimization. We end
				// up here only when the program is being shutdown or when a
				// catastrophic failure occurs. If we succeed in deleting @key
				// then entities watching @key will get notified quickly or else
				// they'll have to wait for the TTL to expire. But if we get here
				// because of catastrophic failure then it's a best effort as the
				// delete can also fail.
				ec.Delete(context.Background(), key, &client.DeleteOptions{})
				close(out)
				return
			}
		}
	}()

	return out
}
