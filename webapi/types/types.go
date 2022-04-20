package types

import (
	"fmt"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
)

// ResponseEnvelope is the structure wrapping all responses from the Evergreen engine
type ResponseEnvelope struct {
	RequestID       string            `json:"request_id,omitempty"`
	ResponseCode    int               `json:"response_code"`
	ErrLines        []string          `json:"error_lines,omitempty"`
	InfoLines       []string          `json:"info_lines,omitempty"`
	ResponseEntries *int              `json:"response_entries,omitempty"`
	Response        []ResponsePayload `json:"response"`
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
	Sources          []DataSource `json:"sources"`
	SampleRequestCmd string       `json:"sample_request_cmd"`
}

type DataSource interface { //nolint:revive
	SrcType() string
	ExpiryUnixNano() int64
	ExpiryCoarse() int64
	SysID() string
}

type FilSource struct { //nolint:revive
	SourceType string `json:"source_type"`
	ProviderID string `json:"provider_id"`

	// filecoin specific
	DealID             int64      `json:"deal_id"`
	OriginalPayloadCid string     `json:"original_payload_cid"`
	DealExpiration     time.Time  `json:"deal_expiration"`
	IsFilplus          bool       `json:"is_filplus"`
	SectorID           *string    `json:"sector_id"`
	SectorExpires      *time.Time `json:"sector_expires"`
	SampleRetrieveCmd  string     `json:"sample_retrieve_cmd"`

	PieceCid             string `json:"-"`
	NormalizedPayloadCid string `json:"-"`

	expUnixNano int64
	expCoarse   int64
	sysIDStr    string
}

var _ DataSource = &FilSource{}

func (s *FilSource) SrcType() string       { return s.SourceType }  //nolint:revive
func (s *FilSource) ExpiryCoarse() int64   { return s.expCoarse }   //nolint:revive
func (s *FilSource) ExpiryUnixNano() int64 { return s.expUnixNano } //nolint:revive
func (s *FilSource) SysID() string         { return s.sysIDStr }    //nolint:revive

func (s *FilSource) InitDerivedVals() { //nolint:revive
	s.SourceType = "Filecoin"
	s.sysIDStr = fmt.Sprintf("%d", s.DealID)
	s.expUnixNano = s.DealExpiration.UnixNano()
	s.expCoarse = s.DealExpiration.Truncate(time.Hour * 24 * 7).UnixNano()

	s.SampleRetrieveCmd = fmt.Sprintf(
		"lotus client retrieve --provider %s --maxPrice 0 --allow-local --car '%s' $(pwd)/%s__%s.car",
		s.ProviderID,
		s.OriginalPayloadCid,
		common.TrimCidString(s.PieceCid),
		common.TrimCidString(s.NormalizedPayloadCid),
	)
}
