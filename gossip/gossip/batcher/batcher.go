/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protos/gossip"
	"github.com/pkg/errors"
)

type emitBatchCallback func([]interface{})

//BatchingEmitter is used for the gossip push/forwarding phase.
// Messages are added into the BatchingEmitter, and they are forwarded periodically T times in batches and then discarded.
// If the BatchingEmitter's stored message count reaches a certain capacity, that also triggers a message dispatch
type BatchingEmitter interface {
	// Add adds a message to be batched
	Add(interface{}, *int32, *int32)

	OnAdvertise(message gossip.ReceivedMessage)
	OnRequest(message gossip.ReceivedMessage)

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
func NewBatchingEmitter(iterations, burstSize int, latency time.Duration, cb emitBatchCallback) BatchingEmitter {
	if iterations < 0 {
		panic(errors.Errorf("Got a negative iterations number"))
	}

	p := &batchingEmitterImpl{
		cb:         cb,
		delay:      latency,
		iterations: iterations,
		burstSize:  burstSize,
		lock:       &sync.Mutex{},
		mapLock:    &sync.Mutex{},
		buff:       make([]*batchedMessage, 0),
		stopFlag:   int32(0),
		nonces:     make(map[uint64]*gossip.GossipMessage),
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

func (p *batchingEmitterImpl) newNONCE() uint64 {
	n := uint64(0)
	for {
		n = util.RandomUInt64()
		if _, ok := p.nonces[n]; !ok {
			return n
		}
	}
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
		if *v.pushesLeft != 0 {
			msgs2bePushed = append(msgs2bePushed, v.data)
		}
		if *v.advertisesLeft != 0 {
			emsg := v.data.(*gossip.EmittedGossipMessage)
			nonce := p.newNONCE()
			msg := &gossip.GossipMessage{
				Nonce:   0,
				Tag:     gossip.GossipMessage_CHAN_AND_ORG,
				Channel: emsg.Channel,
				Content: &gossip.GossipMessage_DataMsg{
					DataMsg: &gossip.DataMessage{
						PushTtl: *v.pushesLeft,
						AdvTtl:  *v.advertisesLeft,
						Payload: emsg.GetDataMsg().Payload,
					},
				},
			}
			amsg := &gossip.EmittedGossipMessage{
				SignedGossipMessage: &gossip.SignedGossipMessage{
					Envelope: nil,
					GossipMessage: &gossip.GossipMessage{
						Nonce:   0,
						Tag:     gossip.GossipMessage_CHAN_AND_ORG,
						Channel: emsg.Channel,
						Content: &gossip.GossipMessage_AdvMsg{
							AdvMsg: &gossip.AdvertiseMessage{
								Nonce:  0,
								SeqNum: emsg.GetDataMsg().Payload.SeqNum,
							},
						},
					},
					Signer: func(msg []byte) ([]byte, error) {
						return nil, nil
					},
				},
				Filter: func(_ common.PKIidType) bool {
					return true
				},
			}
			p.mapLock.Lock()
			p.nonces[nonce] = msg
			p.mapLock.Unlock()
			msgs2beAdvertised = append(msgs2beAdvertised, amsg)
			time.AfterFunc(time.Duration(1000)*time.Millisecond, func() {
				p.mapLock.Lock()
				delete(p.nonces, nonce)
				p.mapLock.Unlock()
			})
		}
	}

	p.decrementCounters()
	p.cb(msgs2bePushed)
	p.cb(msgs2beAdvertised)
	p.updateBuffer()
}

func (p *batchingEmitterImpl) decrementCounters() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		if *msg.pushesLeft != 0 {
			*msg.pushesLeft--
			continue
		}
		*msg.advertisesLeft--
	}
}

func (p *batchingEmitterImpl) updateBuffer() {
	n := len(p.buff)
	for i := 0; i < n; i++ {
		msg := p.buff[i]
		if *msg.pushesLeft == 0 && *msg.advertisesLeft == 0 {
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
	mapLock    *sync.Mutex
	buff       []*batchedMessage
	nonces     map[uint64]*gossip.GossipMessage
	stopFlag   int32
}

type batchedMessage struct {
	data           interface{}
	pushesLeft     *int32
	advertisesLeft *int32
}

func (p *batchingEmitterImpl) Stop() {
	atomic.StoreInt32(&(p.stopFlag), int32(1))
}

func (p *batchingEmitterImpl) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.buff)
}

func (p *batchingEmitterImpl) Add(message interface{}, pPushTtl *int32, pAdvTtl *int32) {
	var pushTtl *int32
	var advTtl *int32

	pushTtl = pPushTtl
	if pPushTtl == nil {
		var iterations int32
		iterations = int32(p.iterations)
		pushTtl = &iterations
	}
	advTtl = pAdvTtl
	if pAdvTtl == nil {
		var iterations int32
		iterations = 0
		advTtl = &iterations
	}

	if *pushTtl == 0 && *advTtl == 0 {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()

	p.buff = append(p.buff, &batchedMessage{data: message, pushesLeft: pushTtl, advertisesLeft: advTtl})

	if len(p.buff) >= p.burstSize {
		p.emit()
	}
}

func (p *batchingEmitterImpl) OnAdvertise(msg gossip.ReceivedMessage) {
	reqMsg := &gossip.GossipMessage{
		Channel: msg.GetGossipMessage().Channel,
		Tag:     msg.GetGossipMessage().Tag,
		Nonce:   0,
		Content: &gossip.GossipMessage_ReqMsg{
			ReqMsg: &gossip.RequestMessage{
				Nonce: msg.GetGossipMessage().GetAdvMsg().Nonce,
			},
		},
	}
	msg.Respond(reqMsg)
}

func (p *batchingEmitterImpl) OnRequest(msg gossip.ReceivedMessage) {
	p.mapLock.Lock()
	resp, ok := p.nonces[msg.GetGossipMessage().GetReqMsg().Nonce]
	p.mapLock.Unlock()
	if ok {
		msg.Respond(resp)
	}
}
