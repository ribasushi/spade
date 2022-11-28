// Package infomempeerstore wraps pstoremem.NewPeerstore() providing
// an extra methods to examine the internal peer state: GetPeerData()
package infomempeerstore

import (
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
)

type PeerStore struct { //nolint:revive
	peerstore.Peerstore
	peerMeta   map[peer.ID]map[string]interface{}
	peerProtos map[peer.ID]map[string]struct{}
	mu         *sync.RWMutex
}

type PeerData struct { //nolint:revive
	Meta   map[string]interface{} `json:"meta"`
	Protos map[string]struct{}    `json:"libp2p_protocols"`
}

func NewPeerstore(opts ...pstoremem.Option) (*PeerStore, error) { //nolint:revive
	ps, err := pstoremem.NewPeerstore(opts...)
	if err != nil {
		return nil, err
	}
	return &PeerStore{
		Peerstore:  ps,
		peerMeta:   make(map[peer.ID]map[string]interface{}, 16),
		peerProtos: make(map[peer.ID]map[string]struct{}, 16),
		mu:         new(sync.RWMutex),
	}, nil
}

func (ps *PeerStore) GetPeerData(p peer.ID) PeerData { //nolint:revive
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	pd := PeerData{
		Meta:   make(map[string]interface{}, len(ps.peerMeta[p])),
		Protos: make(map[string]struct{}, len(ps.peerProtos[p])),
	}
	for k, v := range ps.peerMeta[p] {
		pd.Meta[k] = v
	}
	for k, v := range ps.peerProtos[p] {
		pd.Protos[k] = v
	}
	return pd
}

func (ps *PeerStore) RemovePeer(p peer.ID) { //nolint:revive
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.Peerstore.RemovePeer(p)
	delete(ps.peerProtos, p)
	delete(ps.peerMeta, p)
}

func (ps *PeerStore) Put(p peer.ID, key string, val interface{}) error { //nolint:revive
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.Peerstore.Put(p, key, val); err != nil {
		return err
	}
	if ps.peerMeta[p] == nil {
		ps.peerMeta[p] = make(map[string]interface{}, 2)
	}
	ps.peerMeta[p][key] = val
	return nil
}

func (ps *PeerStore) SetProtocols(p peer.ID, protos ...string) error { //nolint:revive
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.Peerstore.SetProtocols(p, protos...); err != nil {
		return err
	}
	ps.peerProtos[p] = make(map[string]struct{}) // always reset
	for _, pr := range protos {
		ps.peerProtos[p][pr] = struct{}{}
	}
	return nil
}
func (ps *PeerStore) AddProtocols(p peer.ID, protos ...string) error { //nolint:revive
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.Peerstore.AddProtocols(p, protos...); err != nil {
		return err
	}
	if ps.peerProtos[p] == nil {
		ps.peerProtos[p] = make(map[string]struct{})
	}
	for _, pr := range protos {
		ps.peerProtos[p][pr] = struct{}{}
	}
	return nil
}
func (ps *PeerStore) RemoveProtocols(p peer.ID, protos ...string) error { //nolint:revive
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if err := ps.Peerstore.RemoveProtocols(p, protos...); err != nil {
		return err
	}
	if ps.peerProtos[p] != nil {
		for _, pr := range protos {
			delete(ps.peerProtos[p], pr)
		}
	}
	return nil
}

// need to reimplement these two, as they are resolved at runtime 😿
// nolint:revive
func (ps *PeerStore) ConsumePeerRecord(s *record.Envelope, ttl time.Duration) (accepted bool, err error) {
	ab, cast := ps.Peerstore.(peerstore.CertifiedAddrBook)
	if !cast {
		return false, errors.New("peerstore should also be a certified address book")
	}
	return ab.ConsumePeerRecord(s, ttl)
}
func (ps *PeerStore) GetPeerRecord(p peer.ID) *record.Envelope { //nolint:revive
	ab, cast := ps.Peerstore.(peerstore.CertifiedAddrBook)
	if !cast {
		return nil
	}
	return ab.GetPeerRecord(p)
}

var _ peerstore.Peerstore = &PeerStore{}
var _ peerstore.CertifiedAddrBook = &PeerStore{}
