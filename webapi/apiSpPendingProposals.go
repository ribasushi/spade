package main

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/labstack/echo/v4"
)

func apiSpListPendingProposals(c echo.Context) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	type pendingProposals struct {
		types.DealProposal
		pieceSources
		ClientID          cmn.ActorID
		PieceID           int64
		ProposalFailstamp int64
		Error             *string
		ProposalDelivered *time.Time
		IsPublished       bool
		PieceLog2Size     int8
	}
	pending := make([]pendingProposals, 0, 4096)

	if err := pgxscan.Select(
		ctx,
		cmn.Db,
		&pending,
		`
		SELECT
				pr.proposal_uuid AS proposal_id,
				pr.piece_id,
				pr.proposal_meta->>'signed_proposal_cid' AS proposal_cid,
				pr.start_epoch,
				pr.client_id,
				pr.proposal_delivered,
				c.tenant_id,
				p.piece_cid,
				pr.proxied_log2_size AS piece_log2_size,
				pr.proposal_failstamp,
				pr.proposal_meta->>'failure' AS error,
				( EXISTS (
					SELECT 42
						FROM egd.published_deals pd
					WHERE
						pd.piece_id = pr.piece_id
							AND
						pd.provider_id = pr.provider_id
							AND
						pd.client_id = pr.client_id
							AND
						pd.status = 'published'
				) ) AS is_published,
				COALESCE( ( pa.coarse_latest_active_end_epoch IS NOT NULL ), false ) AS has_sources_fil_active,
				false AS has_sources_http
			FROM egd.proposals pr
			JOIN egd.pieces p USING ( piece_id )
			JOIN egd.clients c USING ( client_id )
			LEFT JOIN egd.mv_pieces_availability pa USING ( piece_id )
		WHERE
			pr.provider_id = $1
				AND
			pr.start_epoch > $2
				AND
			pr.activated_deal_id is NULL
				AND
			(
				pr.proposal_failstamp = 0
					OR
				-- show everything failed in the past N hours
				pr.proposal_failstamp > ( egd.big_now() - $3::BIGINT * 3600 * 1000 * 1000 * 1000 )
			)
		ORDER BY
			pr.proposal_failstamp DESC,
			( pr.start_epoch / 360 ), -- 3h sort granularity
			pr.proxied_log2_size,
			p.piece_cid
		`,
		ctxMeta.authedActorID,
		cmn.WallTimeEpoch(time.Now())+filbuiltin.EpochsInHour-28000,
		cmn.ShowRecentFailuresHours,
	); err != nil {
		return cmn.WrErr(err)
	}

	type dealTuple struct {
		pieceID  int64
		tenantID int16
	}

	var toPropose, toActivate, outstandingBytes int64
	srcPtrs := make(piecePointers, len(pending))
	fails := make(map[dealTuple]types.ProposalFailure)
	ret := types.ResponsePendingProposals{
		PendingProposals: make([]types.DealProposal, 0, len(pending)),
	}

	for _, p := range pending {
		outstandingBytes += (1 << p.PieceLog2Size)

		switch {

		case p.IsPublished:
			toActivate++

		case p.ProposalFailstamp > 0:
			t := dealTuple{pieceID: p.PieceID, tenantID: p.TenantID}
			f := types.ProposalFailure{
				ErrorTimeStamp: time.Unix(0, p.ProposalFailstamp),
				Error:          *p.Error,
				PieceCid:       p.PieceCid,
				ProposalID:     p.ProposalID,
				ProposalCid:    p.ProposalCid,
				TenantID:       p.TenantID,
				TenantClient:   p.ClientID.String(),
			}
			prev, seen := fails[t]
			if !seen || prev.ErrorTimeStamp.Before(f.ErrorTimeStamp) {
				fails[t] = f
			}

		case p.ProposalDelivered == nil:
			toPropose++

		default:
			dp := p.DealProposal
			dp.StartTime = cmn.MainnetTime(filabi.ChainEpoch(dp.StartEpoch))
			dp.HoursRemaining = int(time.Until(dp.StartTime).Truncate(time.Hour).Hours())
			dp.PieceSize = 1 << p.PieceLog2Size
			dp.TenantClient = p.ClientID.String()
			// should never be nil but be cautious
			if dp.ProposalCid != nil {
				dp.ImportCmd = fmt.Sprintf("lotus-miner storage-deals import-data %s %s.car",
					*dp.ProposalCid,
					cmn.TrimCidString(dp.PieceCid),
				)
			}

			ret.PendingProposals = append(ret.PendingProposals, dp)

			p.pieceSources.sourcesPointer = &ret.PendingProposals[len(ret.PendingProposals)-1].Sources
			p.pieceSources.pieceCid = p.PieceCid
			srcPtrs[p.PieceID] = p.pieceSources
		}
	}

	if err := injectSources(ctx, srcPtrs, 0); err != nil {
		return cmn.WrErr(err)
	}

	msg := fmt.Sprintf(
		`
This is an overview of deals recently proposed to SP %s

There currently are %0.2f GiB of pending deals:
  % 4d deal-proposals to send out
  % 4d successful proposals pending publishing
  % 4d deals published on chain awaiting sector activation

You can request deal proposals using API endpoints as described in the docs`,
		ctxMeta.authedActorID,
		float64(outstandingBytes)/(1<<30),
		toPropose,
		len(ret.PendingProposals),
		toActivate,
	)

	if len(fails) > 0 {
		msg += fmt.Sprintf("\n\nIn the past %dh there were %d proposal errors, shown in recent_failures below.", cmn.ShowRecentFailuresHours, len(fails))

		ret.RecentFailures = make([]types.ProposalFailure, 0, len(fails))
		for _, f := range fails {
			ret.RecentFailures = append(ret.RecentFailures, f)
		}
		sort.Slice(ret.RecentFailures, func(i, j int) bool {
			return ret.RecentFailures[j].ErrorTimeStamp.Before(ret.RecentFailures[i].ErrorTimeStamp)
		})
	}

	return retPayloadAnnotated(
		c,
		http.StatusOK,
		0,
		ret,
		msg,
	)
}
