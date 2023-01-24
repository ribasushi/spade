// Package filtypes carries definitions for the modern fil-market protocols
// While they were pioneered by Boost, we are trying to avoid a hard dependency
// on the suite of libs, which are much more SP-centric.
package filtypes

import (
	"io"

	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

//go:generate go run github.com/hannahhoward/cbor-gen-for --map-encoding StorageProposalV12xParams StorageProposalV120Response

//nolint:revive
const (
	RetrievalQueryAsk   = "/fil/retrieval/qry/1.0.0"        // use the 1.0 protocol even if we do not care about PCIDs
	RetrievalTransports = "/fil/retrieval/transports/1.0.0" // this is boost-specific, do not bring extra dependency
	StorageProposalV120 = "/fil/storage/mk/1.2.0"           // same: boost-specific
)

// StorageProposalV12xParams is an amalgam of
// https://github.com/filecoin-project/boost/blob/v1.5.0/storagemarket/types/types.go#L80-L84
// and
// https://github.com/filecoin-project/boost/blob/v1.5.1-rc3/storagemarket/types/types.go#L80-L88
type StorageProposalV12xParams struct {
	DealUUID           uuid.UUID
	IsOffline          bool
	ClientDealProposal filmarket.ClientDealProposal
	DealDataRoot       cid.Cid
	RemoveUnsealedCopy bool
	SkipIPNIAnnounce   bool
}

// StorageProposalV120Response is a copy of https://github.com/filecoin-project/boost/blob/v1.5.0/storagemarket/types/types.go#L142-L147
type StorageProposalV120Response struct {
	Accepted bool
	// Message is the reason the deal proposal was rejected.
	// It is empty if the deal was accepted 🤦
	Message string
}

// RetrievalTransports100RawResponse is a copy of https://github.com/filecoin-project/boost/blob/v1.5.0/retrievalmarket/types/transports.go#L12-L21
type RetrievalTransports100RawResponse struct {
	Protocols []struct {
		Name      string
		Addresses [][]byte
	}
}

// cbor-gen does not like [][]byte, so just do things the old way here
//
//nolint:revive
func (rs *RetrievalTransports100RawResponse) UnmarshalCBOR(r io.Reader) error {
	return cbor.NewDecoder(r).Decode(rs)
}
