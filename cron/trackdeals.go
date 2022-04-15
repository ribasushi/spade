package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/evergreen-dealer/common"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	lotusapi "github.com/filecoin-project/lotus/api"
	filprovider "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	filtypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var trackDeals = &cli.Command{
	Usage: "Track state of filecoin deals related to known PieceCIDs",
	Name:  "track-deals",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) (defErr error) {

		ctx := cctx.Context

		var stateTipset *filtypes.TipSet
		var stateDeals map[string]lotusapi.MarketDeal
		dealQueryDone := make(chan error, 1)
		go func() {

			defer close(dealQueryDone)

			var err error
			stateTipset, err = common.LotusLookbackTipset(ctx)
			if err != nil {
				dealQueryDone <- err
				return
			}

			log.Infow("retrieving Market Deals from", "state", stateTipset.Key(), "epoch", stateTipset.Height(), "wallTime", time.Unix(int64(stateTipset.Blocks()[0].Timestamp), 0))
			stateDeals, err = common.LotusAPI.StateMarketDeals(ctx, stateTipset.Key())
			if err != nil {
				dealQueryDone <- err
				return
			}

			log.Infof("retrieved %s state deal records", humanize.Comma(int64(len(stateDeals))))
		}()

		affiliatedClientDatacap := make(map[filaddr.Address]*filbig.Int)
		rows, err := common.Db.Query(
			ctx,
			`SELECT client_address FROM clients WHERE is_affiliated`,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var c string
			if err = rows.Scan(&c); err != nil {
				return err
			}
			cAddr, err := filaddr.NewFromString(c)
			if err != nil {
				return err
			}

			dcap, err := common.LotusAPI.StateVerifiedClientStatus(ctx, cAddr, stateTipset.Key())
			if err != nil {
				return err
			}
			affiliatedClientDatacap[cAddr] = dcap
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		log.Infof("queried datacap for %d clients", len(affiliatedClientDatacap))

		knownPieces := make(map[cid.Cid]struct{}, 5_000_000)

		type filDeal struct {
			pieceCid cid.Cid
			status   string
		}
		knownDeals := make(map[int64]filDeal)

		rows, err = common.Db.Query(
			ctx,
			`
			SELECT p.piece_cid, d.deal_id, d.status
				FROM pieces p
				LEFT JOIN published_deals d USING ( piece_cid )
			`,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var pCidStr string
			var dealID *int64
			var dealStatus *string

			if err = rows.Scan(&pCidStr, &dealID, &dealStatus); err != nil {
				return err
			}
			pCid, err := cid.Parse(pCidStr)
			if err != nil {
				return err
			}
			knownPieces[pCid] = struct{}{}

			if dealID == nil {
				continue
			}

			knownDeals[*dealID] = filDeal{
				pieceCid: pCid,
				status:   *dealStatus,
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		knownProviders := make(map[filaddr.Address]struct{}, 4096)

		var newDealCount, terminatedDealCount int
		dealTotals := make(map[string]int64)
		defer func() {
			log.Infow("summary",
				"knownPieces", len(knownPieces),
				"relatedDeals", dealTotals,
				"totalProviders", len(knownProviders),
				"newlyAdded", newDealCount,
				"newlyTerminated", terminatedDealCount,
			)
		}()

		if len(knownPieces) == 0 {
			return nil
		}

		if err = <-dealQueryDone; err != nil {
			return err
		}

		log.Infof("checking the status of %s known Piece CIDs", humanize.Comma(int64(len(knownPieces))))

		tx, err := common.Db.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if defErr != nil {
				tx.Rollback(context.Background()) //nolint:errcheck
			}
		}()

		clientLookup := make(map[filaddr.Address]filaddr.Address, 32)
		for dealIDString, d := range stateDeals {

			// only consider deals with trackable pcid
			if _, known := knownPieces[d.Proposal.PieceCID]; !known {
				continue
			}

			dealID, err := strconv.ParseInt(dealIDString, 10, 64)
			if err != nil {
				return err
			}

			var initialEncounter bool
			var prevStatus *string
			if kd, known := knownDeals[dealID]; !known {
				initialEncounter = true
			} else {
				// at the end whatever remains is not in SMA list, thus will be marked "terminated"
				prevStatus = &kd.status
				delete(knownDeals, dealID)
			}

			knownProviders[d.Proposal.Provider] = struct{}{}

			if _, seen := clientLookup[d.Proposal.Client]; !seen {
				robust, err := common.LotusAPI.StateAccountKey(ctx, d.Proposal.Client, stateTipset.Key())
				if err != nil {
					return err
				}
				clientLookup[d.Proposal.Client] = robust
			}

			var statusMeta *string
			var sectorStart *filabi.ChainEpoch
			status := "published"
			var termTime *time.Time
			if d.State.SlashEpoch != -1 {
				status = "terminated"

				m := "entered final-slashed state"
				statusMeta = &m

				tn := time.Now()
				termTime = &tn
			} else if d.State.SectorStartEpoch > 0 {
				sectorStart = &d.State.SectorStartEpoch
				status = "active"
				m := fmt.Sprintf(
					"containing sector active as of %s at epoch %d",
					common.MainnetTime(d.State.SectorStartEpoch).Format("2006-01-02 15:04:05"),
					d.State.SectorStartEpoch,
				)
				statusMeta = &m
			} else if d.Proposal.StartEpoch+filprovider.WPoStChallengeWindow < stateTipset.Height() {
				// if things are lookback+one deadlines late: they are never going to make it
				status = "terminated"

				m := fmt.Sprintf(
					"containing sector missed expected sealing epoch %d",
					d.Proposal.StartEpoch,
				)
				statusMeta = &m

				tn := time.Now()
				termTime = &tn
			}

			dealTotals[status]++
			if initialEncounter {
				if status == "terminated" {
					terminatedDealCount++
				} else {
					newDealCount++
				}
			}

			if d.Proposal.VerifiedDeal && status == "published" {
				if dcap, isAc := affiliatedClientDatacap[clientLookup[d.Proposal.Client]]; isAc {
					filbig.Add(*dcap, filbig.NewInt(int64(d.Proposal.PieceSize)))
				}
			}

			// at this point if the status hasn't changed - there is nothing to UPSERT
			if prevStatus != nil && status == *prevStatus {
				continue
			}

			var labelCid *string
			if lc, err := cid.Parse(d.Proposal.Label); err == nil {
				lcs := common.CidV1(lc).String()
				labelCid = &lcs
			}

			_, err = tx.Exec(
				ctx,
				`
				INSERT INTO published_deals
					( deal_id, client_id, provider_id, piece_cid, label, decoded_label, is_filplus, status, status_meta, start_epoch, end_epoch, sector_start_epoch, termination_detection_time )
					VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 )
				ON CONFLICT ( deal_id ) DO UPDATE SET
					status = EXCLUDED.status,
					status_meta = EXCLUDED.status_meta,
					sector_start_epoch = COALESCE( EXCLUDED.sector_start_epoch, published_deals.sector_start_epoch ),
					termination_detection_time = EXCLUDED.termination_detection_time
				`,
				dealID,
				d.Proposal.Client.String(),
				d.Proposal.Provider.String(),
				d.Proposal.PieceCID.String(),
				[]byte(d.Proposal.Label),
				labelCid,
				d.Proposal.VerifiedDeal,
				status,
				statusMeta,
				d.Proposal.StartEpoch,
				d.Proposal.EndEpoch,
				sectorStart,
				termTime,
			)
			if err != nil {
				return err
			}

			if status == "active" && (prevStatus == nil || *prevStatus != "active") {
				if _, err := tx.Exec(
					ctx,
					`
					UPDATE proposals
						SET activated_deal_id = $1
					WHERE
						proposal_failstamp = 0
							AND
						proposal_success_cid IS NOT NULL
							AND
						activated_deal_id IS NULL
							AND
						piece_cid = $2
							AND
						provider_id = $3
							AND
						client_id = $4
					`,
					dealID,
					d.Proposal.PieceCID.String(),
					d.Proposal.Provider.String(),
					d.Proposal.Client.String(),
				); err != nil {
					return err
				}
			}
		}

		// we may have some terminations ( no longer in the market state )
		toFail := make([]int64, 0, len(knownDeals))
		for dID, d := range knownDeals {
			dealTotals["terminated"]++
			if d.status == "terminated" {
				continue
			}
			terminatedDealCount++
			toFail = append(toFail, dID)
		}
		if len(toFail) > 0 {
			_, err = tx.Exec(
				ctx,
				`
				UPDATE published_deals SET
					status = 'terminated',
					status_meta = 'deal no longer part of market-actor state',
					termination_detection_time = NOW()
				WHERE
					deal_id = ANY ( $1::BIGINT[] )
						AND
					status != 'terminated'
				`,
				toFail,
			)
			if err != nil {
				return err
			}
		}

		// set all robust addresses
		for c, r := range clientLookup {
			if _, err = tx.Exec(
				ctx,
				`
				UPDATE clients SET
					client_address = $1
				WHERE
					client_id = $2
				`,
				r.String(),
				c.String(),
			); err != nil {
				return err
			}
		}

		// update datacap
		for c, d := range affiliatedClientDatacap {
			var di *int64
			if d != nil {
				v := d.Int64()
				di = &v
			}
			if _, err = tx.Exec(
				ctx,
				`
				UPDATE clients SET
					activateable_datacap = $1
				WHERE
					client_address = $2
				`,
				di,
				c.String(),
			); err != nil {
				return err
			}
		}

		// clear out proposals that will never make it
		if _, err = tx.Exec(
			ctx,
			`
			UPDATE proposals SET
				proposal_failstamp = ( extract(epoch from CLOCK_TIMESTAMP()) * 1000000000 )::BIGINT,
				meta = JSONB_SET(
					COALESCE( meta, '{}' ),
					'{ failure }',
					TO_JSONB( 'proposal DealStartEpoch ' || (dealstart_payload ->> 'DealStartEpoch') || ' reached at ' || NOW() || ' without activation' )
				)
			WHERE
				proposal_failstamp = 0
					AND
				activated_deal_id IS NULL
					AND
				start_by < NOW()
			`,
		); err != nil {
			return err
		}

		// clear out proposals that had an active deal which subsequently terminated
		if _, err = tx.Exec(
			ctx,
			`
			UPDATE proposals SET
				activated_deal_id = NULL,
				proposal_failstamp = ( extract(epoch from CLOCK_TIMESTAMP()) * 1000000000 )::BIGINT,
				meta = JSONB_SET(
					COALESCE( meta, '{}' ),
					'{ failure }',
					TO_JSONB( 'sector containing deal ' || activated_deal_id || ' terminated' )
				)
			WHERE
				activated_deal_id IN ( SELECT deal_id FROM published_deals WHERE status = 'terminated' )
			`,
		); err != nil {
			return err
		}

		// refresh matviews
		log.Info("refreshing materialized views")
		for _, mv := range []string{
			`known_org_ids`,
			`known_cities`,
			`known_countries`,
			`known_continents`,
			`deallist_eligible`,
			`replica_counts`,
		} {
			if _, err = tx.Exec(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY `+mv); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `ANALYZE `+mv); err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	},
}
