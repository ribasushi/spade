package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filaddr "github.com/filecoin-project/go-address"
	gfm "github.com/filecoin-project/go-fil-markets/storagemarket"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	lotusapi "github.com/filecoin-project/lotus/api"
	filtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
)

func apiRequestPiece(c echo.Context) (defErr error) {
	ctx := c.Request().Context()
	spID := c.Response().Header().Get("X-FIL-SPID")
	spSize, err := strconv.ParseUint(c.Response().Header().Get("X-FIL-SPSIZE"), 10, 64)
	if err != nil {
		return err
	}

	pcidStr := c.Param("pieceCID")
	pCid, err := cid.Parse(pcidStr)
	if err != nil {
		return retFail(c, "", "Requested piece cid '%s' is not valid: %s", pcidStr, err)
	}

	internalReason, err := spIneligibleReason(ctx, spID)
	if err != nil {
		return err
	} else if internalReason != "" {
		return retFail(c, internalReason, ineligibleSpMsg(spID))
	}

	cn := types.ReplicaCounts{MaxSp: 1}
	var isKnownPiece, isMineExpiring bool
	var rCidStr *string
	var customMidnightOffsetMins, curOutstandingBytes, customMaxOutstandingGiB, paddedPieceSize *int64

	tx, err := common.Db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		var thisErr error
		if tx != nil {
			thisErr = tx.Rollback(context.Background())
		}
		if defErr == nil {
			defErr = thisErr
		}
	}()

	_, err = tx.Exec(
		ctx,
		`SELECT PG_ADVISORY_XACT_LOCK( 1234567890111 )`,
	)
	if err != nil {
		return err
	}

	err = tx.QueryRow(
		ctx,
		`
		WITH
			providers_of_active_nonexpiring_deals_for_piece AS (
				SELECT DISTINCT( provider_id )
					FROM published_deals
					JOIN clients c USING ( client_id )
				WHERE
					piece_cid = $1
						AND
					status = 'active'
						AND
					end_time > expiration_cutoff()
						AND
					c.is_affiliated
			),
			providers_of_pending_proposals_for_piece AS (
				SELECT provider_id FROM proposals WHERE piece_cid = $1 AND proposal_failstamp = 0 AND activated_deal_id IS NULL
			),
			providers_in_org AS (
				SELECT provider_id FROM providers WHERE org_id IN ( SELECT org_id FROM providers WHERE provider_id = $2 )
			),
			providers_in_city AS (
				SELECT provider_id FROM providers WHERE city IN ( SELECT city FROM providers WHERE provider_id = $2 )
			),
			providers_in_country AS (
				SELECT provider_id FROM providers WHERE country IN ( SELECT country FROM providers WHERE provider_id = $2 )
			),
			providers_in_continent AS (
				SELECT provider_id FROM providers WHERE continent IN ( SELECT continent FROM providers WHERE provider_id = $2 )
			)

		SELECT

			EXISTS( SELECT 42 FROM pieces WHERE piece_cid = $1 ) AS valid_piece,

			( SELECT padded_size FROM pieces WHERE piece_cid = $1 ) AS padded_size,
			( SELECT payload_cid FROM payloads WHERE piece_cid = $1 ) AS payload_cid,

			(
				SELECT SUM ( p.padded_size )
					FROM pieces p
					JOIN proposals pr USING ( piece_cid )
				WHERE
					pr.proposal_failstamp = 0
						AND
					pr.activated_deal_id IS NULL
						AND
					pr.provider_id = $2
			) AS cur_outstanding_bytes,

			( SELECT (meta->>'max_outstanding_GiB')::INTEGER FROM providers WHERE provider_id = $2 ) AS max_outstanding_gib,
			( SELECT (meta->>'midnight_offset_minutes')::INTEGER FROM providers WHERE provider_id = $2 ) AS midnight_offset_mins,

			(
				SELECT
					EXISTS (
						SELECT 42
							FROM published_deals pd
						WHERE
							piece_cid = $1
								AND
							provider_id = $2
								AND
							(
								pd.status = 'terminated'
									OR
								pd.end_time < expiration_cutoff()
							)
					)

						AND

					-- I do not hold a non-expiring deal
					NOT EXISTS (
						SELECT 42
							FROM published_deals pd
						WHERE
							pd.piece_cid = $1
								AND
							pd.provider_id = $2
								AND
							pd.status != 'terminated'
								AND
							pd.end_time > expiration_cutoff()
					)
			) AS is_mine_expiring,

			(
				(
					SELECT COUNT(*) FROM providers_of_active_nonexpiring_deals_for_piece
				)
					+
				(
					SELECT COUNT(*) FROM providers_of_pending_proposals_for_piece
				)
			) AS count_total,

			(
				(
					SELECT COUNT(*) FROM providers_of_active_nonexpiring_deals_for_piece WHERE provider_id = $2
				)
					+
				(
					SELECT COUNT(*) FROM providers_of_pending_proposals_for_piece WHERE provider_id = $2
				)
			) AS count_self,

			(
				(
					SELECT COUNT(*) FROM providers_in_org JOIN providers_of_active_nonexpiring_deals_for_piece USING ( provider_id )
				)
					+
				(
					SELECT COUNT(*) FROM providers_in_org JOIN providers_of_pending_proposals_for_piece USING ( provider_id )
				)
			) AS count_within_org,

			(
				(
					SELECT COUNT(*) FROM providers_in_city JOIN providers_of_active_nonexpiring_deals_for_piece USING ( provider_id )
				)
					+
				(
					SELECT COUNT(*) FROM providers_in_city JOIN providers_of_pending_proposals_for_piece USING ( provider_id )
				)
			) AS count_within_city,

			(
				(
					SELECT COUNT(*) FROM providers_in_country JOIN providers_of_active_nonexpiring_deals_for_piece USING ( provider_id )
				)
					+
				(
					SELECT COUNT(*) FROM providers_in_country JOIN providers_of_pending_proposals_for_piece USING ( provider_id )
				)
			) AS count_within_country,

			(
				(
					SELECT COUNT(*) FROM providers_in_continent JOIN providers_of_active_nonexpiring_deals_for_piece USING ( provider_id )
				)
					+
				(
					SELECT COUNT(*) FROM providers_in_continent JOIN providers_of_pending_proposals_for_piece USING ( provider_id )
				)
			) AS count_within_continent,

			max_program_replicas(),
			max_per_org(),
			max_per_city(),
			max_per_country(),
			max_per_continent()
		`,
		pcidStr,
		spID,
	).Scan(
		&isKnownPiece, &paddedPieceSize, &rCidStr,
		&curOutstandingBytes, &customMaxOutstandingGiB, &customMidnightOffsetMins,
		&isMineExpiring, &cn.Total, &cn.Self, &cn.InOrg, &cn.InCity, &cn.InCountry, &cn.InContinent,
		&cn.MaxTotal, &cn.MaxOrg, &cn.MaxCity, &cn.MaxCountry, &cn.MaxContinent,
	)
	if err != nil {
		return err
	}

	if !isKnownPiece {
		return retFail(c, nil, "Piece CID '%s' is not known to the system", pcidStr)
	}
	if uint64(*paddedPieceSize) > spSize {
		return retFail(c, nil, "Piece CID '%s' weighing %d GiB is larger than the %d GiB sector size your SP supports", pcidStr, *paddedPieceSize>>30, spSize>>30)
	}

	rCid, err := cid.Parse(*rCidStr)
	if err != nil {
		return err
	}

	maxBytes := common.MaxOutstandingGiB
	if customMaxOutstandingGiB != nil && *customMaxOutstandingGiB > common.MaxOutstandingGiB {
		maxBytes = *customMaxOutstandingGiB
	}
	maxBytes <<= 30

	if curOutstandingBytes == nil {
		curOutstandingBytes = new(int64)
	}

	r := types.ResponseDealRequest{CurOutstandingBytes: *curOutstandingBytes}

	if *curOutstandingBytes >= maxBytes {
		return retPayloadAnnotated(
			c,
			403,
			r,
			"SPS %s has more deals currently in flight than permitted by the system.\nTry again after you have activated some of your existing proposals.",
			spID,
		)
	}

	r.MaxOutstandingBytes = &maxBytes
	r.TentativeCounts = cn
	if cn.Self >= cn.MaxSp ||
		cn.InOrg >= cn.MaxOrg ||
		(!isMineExpiring &&
			(cn.Total >= cn.MaxTotal ||
				cn.InCity >= cn.MaxCity ||
				cn.InCountry >= cn.MaxCountry ||
				cn.InContinent >= cn.MaxContinent)) {

		return retPayloadAnnotated(
			c,
			403,
			r,
			"The current distribution of replicas for %s violates one of the program rules",
			pcidStr,
		)
	}

	// let's do it!
	now := time.Now().UTC()
	lastMidnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if customMidnightOffsetMins != nil {
		lastMidnight = lastMidnight.Add(time.Minute * time.Duration(*customMidnightOffsetMins%10))
	}
	lastMidnightEpoch := common.WallTimeEpoch(lastMidnight)

	lastMidnightTs, err := common.LotusAPI.ChainGetTipSetByHeight(ctx, lastMidnightEpoch, filtypes.TipSetKey{})
	if err != nil {
		return err
	}

	collateralGiB, err := common.LotusAPI.StateDealProviderCollateralBounds(ctx, filabi.PaddedPieceSize(1<<30), true, lastMidnightTs.Key())
	if err != nil {
		return err
	}
	// make it 1.7 times larger, so that fluctuations in the state won't prevent the deal from being proposed/published later
	// capped by https://github.com/filecoin-project/lotus/blob/v1.13.2-rc2/markets/storageadapter/provider.go#L267
	// and https://github.com/filecoin-project/lotus/blob/v1.13.2-rc2/markets/storageadapter/provider.go#L41
	inflatedCollateralGiB := filbig.Div(
		filbig.Product(
			collateralGiB.Min,
			filbig.NewInt(17),
		),
		filbig.NewInt(10),
	)

	spAddr, err := filaddr.NewFromString(spID)
	if err != nil {
		return err
	}

	dp := lotusapi.StartDealParams{
		DealStartEpoch:    lastMidnightEpoch + common.ProposalStartDelayFromMidnight,
		MinBlocksDuration: common.ProposalDuration,
		FastRetrieval:     true,
		VerifiedDeal:      true,
		Wallet:            common.EgWallet,
		Miner:             spAddr,
		EpochPrice:        filbig.Zero(),
		ProviderCollateral: filbig.Div(
			filbig.Mul(inflatedCollateralGiB, filbig.NewInt(*paddedPieceSize)),
			filbig.NewInt(1<<30),
		),
		Data: &gfm.DataRef{
			TransferType: gfm.TTManual,
			PieceCid:     &pCid,
			Root:         rCid,
			PieceSize:    filabi.PaddedPieceSize(*paddedPieceSize).Unpadded(),
		},
	}

	dpJ, err := json.Marshal(dp)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO proposals
			( provider_id, client_id, piece_cid, dealstart_payload )
		VALUES ( $1, $2, $3, $4 )
		`,
		spID,
		common.EgWallet.String(),
		pcidStr,
		dpJ,
	)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	tx = nil

	// we managed - bump the counts and return stats
	r.CurOutstandingBytes += *paddedPieceSize
	r.TentativeCounts.Self++
	r.TentativeCounts.Total++
	r.TentativeCounts.InOrg++
	r.TentativeCounts.InCity++
	r.TentativeCounts.InCountry++
	r.TentativeCounts.InContinent++

	return retPayloadAnnotated(
		c,
		200,
		r,
		strings.Join([]string{
			fmt.Sprintf("Deal queued for pcid %s", pcidStr),
			``,
			`In about 5 minutes check the pending list:`,
			fmt.Sprintf(` echo curl -sLH "Authorization: $( ./fil-spid.bash %s )" 'https://api.evergreen.filecoin.io/pending_proposals' | sh `, spID),
		}, "\n"),
	)
}
