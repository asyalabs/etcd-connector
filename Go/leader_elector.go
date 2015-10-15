package etcd_recipes

import (
	"errors"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/coreos/etcd/client"
)

type LeaderElector struct {
	ec      *EtcdConnector
	keyPath string
	value   string
	obsvr   *Observer
	ctx     context.Context
	cancel  context.CancelFunc
}

type ElectionResponse struct {
	err error
}

const LE_SLEEP_INTERVAL = 5 * time.Second

func (ec *EtcdConnector) NewLeaderElector(key, val string) *LeaderElector {
	return &LeaderElector{
		ec:      ec,
		keyPath: key,
		value:   val,
		obsvr:   ec.NewObserver(key),
		ctx:     nil,
		cancel:  nil,
	}
}

func (le *LeaderElector) acquireLeadership() (*client.Response, error) {
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, TTL: TTL_VAL}
	return le.ec.Set(le.ctx, le.keyPath, le.value, opts)
}

func (le *LeaderElector) waitForLeadership(waitIndex uint64) (*client.Response, error) {
	var resp *client.Response = nil
	var err error = nil

	wData, err := le.obsvr.Start(waitIndex, false)
	if err != nil {
		return nil, err
	}

	for d := range wData {
		if d.err != nil {
			le.obsvr.Stop()
			err = d.err
			break
		} else {
			if d.Response.Action == "update" || d.Response.Action == "set" {
				waitIndex = d.Response.Node.ModifiedIndex
				continue
			} else if d.Response.Action == "expire" || d.Response.Action == "delete" {
				le.obsvr.Stop()
				resp = d.Response
				break
			}
		}
	}

	return resp, err
}

func (le *LeaderElector) Start() <-chan ElectionResponse {
	var waitIndex uint64 = 0

	out := make(chan ElectionResponse, 2)

	await := make(chan bool, 2)
	renew := make(chan bool, 2)

	le.ctx, le.cancel = context.WithCancel(context.Background())
	if le.ctx == nil || le.cancel == nil {
		out <- ElectionResponse{
			err: errors.New("Couldn't instantiate context/cancel objects"),
		}
		close(out)
		return out
	}

	go func() {
		for {
			select {
			case <-le.ctx.Done():
				le.obsvr.Stop()
				close(out)
				return
			case <-await:
				resp, err := le.waitForLeadership(waitIndex)
				if err != nil {
					time.Sleep(LE_SLEEP_INTERVAL)
				} else if resp != nil {
					waitIndex = resp.Node.ModifiedIndex
				}
			case <-renew:
				errChan := le.ec.RenewTTL(le.ctx, le.keyPath, le.value, TTL_VAL, TTL_REFRESH_TIMEOUT)
				for e := range errChan {
					out <- ElectionResponse{err: e}
					time.Sleep(LE_SLEEP_INTERVAL)
					break
				}
			default:
				_, err := le.acquireLeadership()
				if err != nil {
					errObj, ok := err.(client.Error)
					if ok == true && errObj.Code == client.ErrorCodeNodeExist {
						await <- true
					} else {
						time.Sleep(LE_SLEEP_INTERVAL)
					}
				} else {
					renew <- true
					out <- ElectionResponse{err: nil}
				}
			}
		}
	}()

	return out
}

func (le *LeaderElector) Stop() {
	if le.cancel != nil {
		le.cancel()
	}
}
