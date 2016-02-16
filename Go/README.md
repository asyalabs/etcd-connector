# etcd-connector
A simple wrapper for the etcd client that also provides a set of common distributed
coordination recipes.

## Introduction
*etcd-connector* aims to provide the user a set of recipes that are commonly used
in a distributed systems project. The *etcd-connector* wraps the *etcd-client* which
is used to talk to the *etcd* service.

*etcd* is an open source distributed key-value store that is based on Raft consensus
protocol. *etcd* is typically used to store cluster wide configuration information and
to perform cluster wide coordination operations. The project is implemented & maintained
by CoreOS and more information about the project can be found [here](https://coreos.com/etcd).

### Implementation Details
Currently the *etcd-connector* has been implemented in golang. Although similar schemes
can be used and implemented in other programming languages.

As mentioned earlier the *etcd-connector* wraps the *etcd-client*, that comes with *etcd*,
by including it as an anonymous field. This enables the user to perform all the *etcd-client*
operations using the *etcd-connector* instance. It also exposes a set of new methods that are
used to instantiate the following recipes:

* [Leader Elector](#le)
* [Service Tracker](#st)
* [Ephemeral Key](#ek)
* [Observer](#obs)
* [Health Monitor](#hm)

The *etcd-connector* works with v2 etcd APIs. It has been tested against etcd version 2.2.1

### Getting etcd-connector
As the *etcd-connector* uses *etcd-client* the user will have to execute the following
commands to get access to the code:

```
go get github.com/coreos/etcd/client
go get github.com/asyalabs/etcd-connector
```

Once the etcd-connector code base if fetched the consumer can instantiate the connector
in the following way:

```
import etcdc "github.com/asyalabs/etcd-connector/Go"

server_list := etcdc.PrepareEndpointList([]string{"host1", "hosts2", "host3"}, 4001)
conn, _ := etcdc.NewEtcdConnector(server_list, "my_namespace")
```
The parameter "my_namespace" is essentially a directory under *etcd root ("/")*. This
way applications can define their namespace and operate within that thereby enabling
multiple applications to use the same etcd service.

## Recipes
The following sections provide a brief overview of all the recipes.

### Leader Elector<a name="le"></a>
A recipe to elect a leader amongst a set of participants. All the participants race to
create a key at an agreed upon path in the etcd namespace. Only one of them will succeed
and assume the position of the leader. The leader will then renew the TTL on the key to
stay elected. The other participants wait for the current leader to give up. When the
leader dies the TTL on the key expires and the followers will try to take the leader role.

The following sample snippet describes the usage:
```
ttl := 10 * time.Second

// Construct the key path within the namespace.
keyPath := conn.ConstructPath("elections", "service_name")

// Instantiate a leader elector recipe instance.
ldrElect := conn.NewLeaderElector(keyPath, "participant identity", ttl)

// Start participating.
statusCh, _ := ldrElect.Start()

// Listen to the election response.
go func() {
	for eleResp := range statusCh {
		if eleResp.Status == etcdc.LEStatusLeader {
			// Perform leader activities.
		} else if eleResp.Status == etcdc.LEStatusFollower {
			// Perform follower activities.
		}
	}
}()
...
...
// Stop participating.
ldrElect.Stop()
```

### Service Tracker<a name="st"></a>
A recipe to perform client-side service discovery. In a distributed system multiple
instances of the same service will be running in order to be highly available. Hence
the consumers will need a mechanism to know where those instances are running.

The ServiceTracker can be used to achieve that. The multiple instances of a service
will register themselves with the etcd service at an agreed upon path. The clients of
that service will instantiate a ServiceTracker recipe in order to discover/track the
service instances.

The following sample snippet describes the usage:
```
// Construct the path to the location where the services would have registered.
servicePath := conn.ConstructPath("services", "service_name")

// Instantiate a service tracker recipe instance.
svcTracker := conn.NewServiceTracker(servicePath)

// Begin tracking.
svcDataCh, _ := svcTracker.Start()

// Listen to the changes.
go func() {
	for svcData := range svcDataCh {
		// svcData.Services field contains a map of all the instances.
	}
}()
...
...
// End tracker.
svcTracker.Stop()
```

### Ephemeral Key<a name="ek"></a>
A recipe to create a key in etcd that would be present till the time the service that
created the key is alive. This is similar to the ephemeral znodes concept in the Apache
Zookeeper world. This is very handy while performing liveness tracking. A service announces
its existence by creating a key in etcd with a TTL value set. As long as the service is alive
it keeps renewing the TTL of the key. When the service dies the TTL of the key would eventually
expire causing the key to be deleted thus notifying the component interested in its liveness.

The following sample snippet describes the usage:
```
// Construct the path and instantiate an ephemeral key instance.
path := conn.ConstructPath("services/service_name", "instance1")
ephKey := conn.NewEphemeralKey(path)

// Create the key by setting a value and TTL to it.
ttl := 10 * time.Second
errCh, _ := ephKey.Create("value describing the service instance (<IP:Port#>)", ttl)

// Listen for errors.
go func() {
	for err := range errCh {
		// Take appropriate action.
	}
}()
...
...
// Update the key contents.
err := ephKey.Update("some other value")
...
...
// Delete the ephemeral key.
ephKey.Delete()
```

### Observer<a name="obs"></a>
A simple recipe that wraps etcd's Watcher construct. All the changes that occur to a given key
will be sent back to the caller via a channel. The user can specify the etcd index whence the
observation should start.

The following sample snipper describes the usage:
```
// Instantiate an Observer instance.
obsvr := conn.NewObserver(path)

// Start the observation with recursive flag set to true and waitIndex of 0 (from beginning).
recursive := true
waitIndex := 0
oRespCh, _ := obsvr.Start(waitIndex, recursive)

// Look out for changes.
go func() {
	for oResp := range oRespCh {
		// Take action.
	}
}()
...
...
// Stop the observer.
obsvr.Stop()
```

### Health Monitor<a name="hm"></a>
A recipe that provides an API to monitor the health of the etcd cluster. This is done by querying
the etcd's "/health" endpoint. The implementation is same as the "cluster-health" command of
etcdctl tool. The source can be found here: github.com/coreos/etcd/etcdctl/command/cluster_health.go

The following sample snippet describes the usage:
```
// Instantiate a health monitor instance.
hMon := conn.NewHealthMonitor()

// Specify whether the monitor needs to run once and if not what the scan frequency should be.
once := false
scanFreq := 10 * time.Second

// Start monitoring.
monCh := hMon.Start(once, scanFreq)

// Look for updates.
go func() {
	for status := range monCh {
		if status == etcdc.ClusterStatusHealthy {
			// Cluster health is good. Carry out regular operations. 
		} else if status == etcdc.ClusterStatusUnhealthy {
			// Cluster health is bad.
		}
	}
}()
...
...
// Stop monitoring the cluster health.
hMon.Stop()
```