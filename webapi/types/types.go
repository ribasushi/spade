package types

//go:generate stringer -type=APIErrorCode -output=types_err.go

import (
	"fmt"
	"time"
)

type APIErrorCode int //nolint:revive

// List of known/expected error codes
// re-run `go generate ./...` when updating
const (
	ErrInvalidRequest     APIErrorCode = 4400
	ErrUnauthorizedAccess APIErrorCode = 4401

	ErrSystemTemporarilyDisabled APIErrorCode = 4001
	ErrTenantsOutOfDatacap       APIErrorCode = 4009

	ErrStorageProviderSuspended        APIErrorCode = 4010
	ErrStorageProviderIneligibleToMine APIErrorCode = 4011
	ErrStorageProviderAboveMaxPending  APIErrorCode = 4012

	ErrUnclaimedPieceCID          APIErrorCode = 4020
	ErrOversizedPiece             APIErrorCode = 4021
	ErrExternalReservationRefused APIErrorCode = 4029

	ErrReplicaAlreadyActive  APIErrorCode = 4031
	ErrReplicaAlreadyPending APIErrorCode = 4032
	ErrTooManyReplicas       APIErrorCode = 4033
)

// ResponseEnvelope is the structure wrapping all responses from the Evergreen engine
type ResponseEnvelope struct {
	RequestID          string          `json:"request_id,omitempty"`
	ResponseTime       time.Time       `json:"response_timestamp"`
	ResponseStateEpoch int64           `json:"response_state_epoch,omitempty"`
	ResponseCode       int             `json:"response_code"`
	ErrCode            int             `json:"error_code,omitempty"`
	ErrSlug            string          `json:"error_slug,omitempty"`
	ErrLines           []string        `json:"error_lines,omitempty"`
	InfoLines          []string        `json:"info_lines,omitempty"`
	ResponseEntries    *int            `json:"response_entries,omitempty"`
	Response           ResponsePayload `json:"response"`
}

type isResponsePayload struct{}

type ResponsePayload interface { //nolint:revive
	is() isResponsePayload
}

// ResponsePendingProposals is the response payload returned by the .../pending_proposals endpoint
type ResponsePendingProposals struct {
	RecentFailures      []ProposalFailure `json:"recent_failures,omitempty"`
	CurOutstandingBytes int64             `json:"bytes_pending_current"`
	MaxOutstandingBytes *int64            `json:"bytes_pending_max"`
	PendingProposals    []DealProposal    `json:"pending_proposals"`
}

// ResponseDealRequest is the response payload returned by the .../request_piece/{{PieceCid}} endpoint
type ResponseDealRequest struct {
	TentativeCounts     ReplicaCounts `json:"tentative_replica_counts"`
	CurOutstandingBytes int64         `json:"bytes_pending_current"`
	MaxOutstandingBytes *int64        `json:"bytes_pending_max"`
}

// ResponsePiecesEligible is the response payload returned by the .../eligible_pieces/{{sp_local|anywhere}} endpoints
type ResponsePiecesEligible []*Piece

func (ResponsePendingProposals) is() isResponsePayload { return isResponsePayload{} }
func (ResponseDealRequest) is() isResponsePayload      { return isResponsePayload{} }
func (ResponsePiecesEligible) is() isResponsePayload   { return isResponsePayload{} }

type ProposalFailure struct { //nolint:revive
	Tstamp   time.Time `json:"timestamp"`
	Err      string    `json:"error"`
	PieceCid string    `json:"piece_cid"`
	RootCid  string    `json:"root_cid"`
}

type DealProposal struct { //nolint:revive
	DealCid        string       `json:"deal_proposal_cid"`
	HoursRemaining int          `json:"hours_remaining"`
	PieceSize      int64        `json:"piece_size"`
	PieceCid       string       `json:"piece_cid"`
	RootCid        string       `json:"root_cid"`
	StartTime      time.Time    `json:"deal_start_time"`
	StartEpoch     int64        `json:"deal_start_epoch"`
	ImportCMD      string       `json:"sample_import_cmd"`
	Sources        []DataSource `json:"sources,omitempty"`
}

type ReplicaCounts struct { //nolint:revive
	Total        int64 `json:"actual_total"`
	InOrg        int64 `json:"actual_within_org"`
	InCity       int64 `json:"actual_within_city"`
	InCountry    int64 `json:"actual_within_country"`
	InContinent  int64 `json:"actual_within_continent"`
	Self         int64 `json:"actual_within_this_sp"`
	MaxTotal     int64 `json:"program_max_total"`
	MaxOrg       int64 `json:"program_max_per_org"`
	MaxCity      int64 `json:"program_max_per_city"`
	MaxCountry   int64 `json:"program_max_per_country"`
	MaxContinent int64 `json:"program_max_per_continent"`
	MaxSp        int64 `json:"program_max_per_sp"`
}

type Piece struct { //nolint:revive
	PieceCid         string       `json:"piece_cid"`
	Dataset          *string      `json:"dataset"`
	PaddedPieceSize  uint64       `json:"padded_piece_size"`
	PayloadCids      []string     `json:"payload_cids"`
	SampleRequestCmd string       `json:"sample_request_cmd"`
	Sources          []DataSource `json:"sources,omitempty"`
}

type DataSource interface { //nolint:revive
	SrcType() string
}

type FilSource struct { //nolint:revive
	SourceType string `json:"source_type"`

	// filecoin specific
	DealID             int64      `json:"deal_id"`
	ProviderID         string     `json:"provider_id"`
	OriginalPayloadCid string     `json:"original_payload_cid"`
	DealExpiration     time.Time  `json:"deal_expiration"`
	IsFilplus          bool       `json:"is_filplus"`
	SectorID           *string    `json:"sector_id,omitempty"`
	SectorExpiration   *time.Time `json:"sector_expiration,omitempty"`
	SampleRetrieveCmd  string     `json:"sample_retrieve_cmd"`
}

func (s *FilSource) SrcType() string { return s.SourceType } //nolint:revive
var _ DataSource = &FilSource{}

func (s *FilSource) InitDerivedVals(pieceCid string) error { //nolint:revive
	s.SourceType = "Filecoin"

	if s.ProviderID == "" || s.OriginalPayloadCid == "" {
		return fmt.Errorf("filecoin source object missing mandatory values: %#v", s)
	}

	s.SampleRetrieveCmd = fmt.Sprintf(
		"lotus client retrieve --provider %s --maxPrice 0 --allow-local --car '%s' $(pwd)/%s__%s.car",
		s.ProviderID,
		s.OriginalPayloadCid,
		TrimCidString(pieceCid),
		TrimCidString(s.OriginalPayloadCid),
	)

	return nil
}

const (
	cidTrimPrefix = 6
	cidTrimSuffix = 8
)

func TrimCidString(cs string) string { //nolint:revive
	if len(cs) <= cidTrimPrefix+cidTrimSuffix+2 {
		return cs
	}
	return cs[0:cidTrimPrefix] + "~" + cs[len(cs)-cidTrimSuffix:]
}
