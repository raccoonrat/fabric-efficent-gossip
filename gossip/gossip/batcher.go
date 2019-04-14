/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/protos/gossip"
	"github.com/pkg/errors"
)

type emitBatchCallback func([]interface{})

//BatchingEmitter is used for the gossip push/forwarding phase.
// Messages are added into the BatchingEmitter, and they are forwarded periodically T times in batches and then discarded.
// If the BatchingEmitter's stored message count reaches a certain capacity, that also triggers a message dispatch
type BatchingEmitter interface {
	// Add adds a message to be batched
	Add(interface{}, int32, int32)

	OnAdvertise()
	OnRequest()

	// Stop stops the component
	Stop()

	// Size returns the amount of pending messages to be emitted
	Size() int
}

// newBatchingEmitter accepts the following parameters:
// iterations: number of times each message is forwarded
// burstSize: a threshold that triggers a forwarding because of message count
// latency: the maximum delay that each message can be stored without being forwarded
// cb: a callback that is called in order for the forwarding to take place
func newBatchingEmitter(iterations, burstSize int, latency time.Duration, cb emitBatchCallback) BatchingEmitter {
	if iterations < 0 {
		panic(errors.Errorf("Got a negative iterations number"))
	}

	p := &batchingEmitterImpl{
		cb:         cb,
		delay:      latency,
		iterations: iterations,
		burstSize:  burstSize,
		lock:       &sync.Mutex{},
		buff:       make([]*batchedMessage, 0),
		stopFlag:   int32(0),
	}

	if iterations != 0 {
		go p.periodicEmit()
	}

	return p
}

func (p *batchingEmitterImpl) periodicEmit() {
	for !p.toDie() {
		time.Sleep(p.delay)
		p.lock.Lock()
		p.emit()
		p.lock.Unlock()
	}
}

func (p *batchingEmitterImpl) advertise(msgs []interface{}) {
}

func (p *batchingEmitterImpl) emit() {
	if p.toDie() {
		return
	}
	if len(p.buff) == 0 {
		return
	}
	msgs2bePushed := make([]interface{}, 0)
	msgs2beAdvertised := make([]interface{}, 0)
	for _, v := range p.buff {
		if v.pushesLeft != 0 {
			msgs2bePushed = append(msgs2bePushed, v.data)
		}
		if v.advertisesLeft != 0 {
			msgs2beAdvertised = append(msgs2beAdvertised, v.data)
		}
	}

	p.decrementCounters()
	p.cb(msgs2bePushed)
	p.updateBuffer()
}

func (p *batchingEmitterImpl) decrementCounters() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		if msg.pushesLeft != 0 {
			msg.pushesLeft--
			continue
		}
		msg.advertisesLeft--
	}
}

func (p *batchingEmitterImpl) updateBuffer() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		if msg.pushesLeft == 0 && msg.advertisesLeft == 0 {
			p.buff = append(p.buff[:i], p.buff[i+1:]...)
			n--
			i--
		}
	}
}

func (p *batchingEmitterImpl) toDie() bool {
	return atomic.LoadInt32(&(p.stopFlag)) == int32(1)
}

type batchingEmitterImpl struct {
	iterations int
	burstSize  int
	delay      time.Duration
	cb         emitBatchCallback
	lock       *sync.Mutex
	buff       []*batchedMessage
	nonces     map[uint64]*batchedMessage
	stopFlag   int32
}

type batchedMessage struct {
	data           interface{}
	pushesLeft     int32
	advertisesLeft int32
}

func (p *batchingEmitterImpl) Stop() {
	atomic.StoreInt32(&(p.stopFlag), int32(1))
}

func (p *batchingEmitterImpl) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.buff)
}

func (p *batchingEmitterImpl) Add(message interface{}, pushTtl int32, advTtl int32) {
	if pushTtl == -1 {
		pushTtl = int32(p.iterations)
	}
	if advTtl == -1 {
		advTtl = 0
	}
	if pushTtl == 0 && advTtl == 0 {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	p.buff = append(p.buff, &batchedMessage{data: message, pushesLeft: pushTtl, advertisesLeft: advTtl})

	if len(p.buff) >= p.burstSize {
		p.emit()
	}
}

func (p *batchingEmitterImpl) OnAdvertise() {
	/*msg := gossip.SignedGossipMessage{

	}
	*/
}

func (p *batchingEmitterImpl) OnRequest() {
	var msg gossip.SignedGossipMessage
	if _, ok := p.nonces[msg.GetAdvMsg().Nonce]; ok {
		resp := p.nonces[msg.GetAdvMsg().Nonce]
		msgs2beEmitted := make([]interface{}, 1)
		msgs2beEmitted[0] = resp.data
		p.cb(msgs2beEmitted)
	}
}
