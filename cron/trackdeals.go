package main

import (
	"encoding/json"
	"math/bits"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v4"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"github.com/ribasushi/spade/internal/app"
	"golang.org/x/xerrors"
)

var trackDeals = &ufcli.Command{
	Usage: "Track state of fil deals related to known PieceCIDs",
	Name:  "track-deals",
	Flags: []ufcli.Flag{},
	Action: func(cctx *ufcli.Context) error {
		ctx, log, db, gctx := app.UnpackCtx(cctx.Context)

		curTipset, err := app.DefaultLookbackTipset(ctx)
		if err != nil {
			return cmn.WrErr(err)
		}

		var stateDeals map[string]*lotusapi.MarketDeal
		dealQueryDone := make(chan error, 1)
		go func() {
			defer close(dealQueryDone)
			log.Infow("retrieving Market Deals from", "state", curTipset.Key(), "epoch", curTipset.Height(), "wallTime", time.Unix(int64(curTipset.Blocks()[0].Timestamp), 0))
			stateDeals, err = gctx.LotusAPI[app.FilHeavy].StateMarketDeals(ctx, curTipset.Key())
			if err != nil {
				dealQueryDone <- cmn.WrErr(err)
				return
			}
			log.Infof("retrieved %s state deal records", humanize.Comma(int64(len(stateDeals))))
		}()

		tenantClients := make([]fil.ActorID, 0, 32)
		if err := pgxscan.Select(
			ctx,
			db,
			&tenantClients,
			`SELECT client_id FROM spd.clients WHERE tenant_id IS NOT NULL`,
		); err != nil {
			return cmn.WrErr(err)
		}

		tenantClientDatacap := make(map[filaddr.Address]*filbig.Int, len(tenantClients))
		for _, c := range tenantClients {
			dcap, err := gctx.LotusAPI[app.FilLite].StateVerifiedClientStatus(ctx, c.AsFilAddr(), curTipset.Key())
			if err != nil {
				return cmn.WrErr(err)
			}
			tenantClientDatacap[c.AsFilAddr()] = dcap
		}

		log.Infof("queried datacap for %d clients", len(tenantClientDatacap))

		type filDeal struct {
			pieceID  int64
			pieceCid cid.Cid
			status   string
		}

		// entries from this list are deleted below as we process the new state
		initialDbDeals := make(map[int64]filDeal)

		rows, err := db.Query(
			ctx,
			`
			SELECT d.deal_id, d.piece_id, d.piece_cid, d.status
				FROM spd.published_deals d
			`,
		)
		if err != nil {
			return cmn.WrErr(err)
		}
		defer rows.Close()
		for rows.Next() {
			var dID int64
			var d filDeal
			var pcidStr string

			if err = rows.Scan(&dID, &d.pieceID, &pcidStr, &d.status); err != nil {
				return cmn.WrErr(err)
			}
			if d.pieceCid, err = cid.Parse(pcidStr); err != nil {
				return cmn.WrErr(err)
			}
			initialDbDeals[dID] = d
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return cmn.WrErr(err)
		}

		log.Infof("retrieved %s existing deal records", humanize.Comma(int64(len(initialDbDeals))))

		dealCountsByState := make(map[string]int64, 8)
		seenPieces := make(map[cid.Cid]struct{}, 1<<20)
		seenProviders := make(map[filaddr.Address]struct{}, 4096)
		seenClients := make(map[filaddr.Address]struct{}, 4096)

		defer func() {
			log.Infow("summary",
				"totalDeals", dealCountsByState,
				"uniquePieces", len(seenPieces),
				"uniqueProviders", len(seenProviders),
				"uniqueClients", len(seenClients),
			)
		}()

		// wait for finish, blocking
		if err = <-dealQueryDone; err != nil {
			return cmn.WrErr(err)
		}

		type deal struct {
			*lotusapi.MarketDeal
			dealID            int64
			pieceID           int64
			providerID        fil.ActorID
			clientID          fil.ActorID
			pieceLog2Size     uint8
			prevState         *filDeal
			sectorStart       *filabi.ChainEpoch
			status            string
			terminationReason string
			decodedLabel      *string
			label             []byte
			metaJSONB         []byte
		}

		toUpsert := make([]*deal, 0, 8<<10)

		for dealIDString, protoDeal := range stateDeals {

			d := deal{
				MarketDeal: protoDeal,
				status:     "published", // always begin as "published" adjust accordingly below
			}

			d.dealID, err = strconv.ParseInt(dealIDString, 10, 64)
			if err != nil {
				return cmn.WrErr(err)
			}

			if kd, known := initialDbDeals[d.dealID]; known {
				d.prevState = &kd
				delete(initialDbDeals, d.dealID) // at the end whatever remains is not in SMA list, thus will be marked "terminated"
			}

			seenPieces[d.Proposal.PieceCID] = struct{}{}
			seenProviders[d.Proposal.Provider] = struct{}{}
			seenClients[d.Proposal.Client] = struct{}{}

			if d.State.SlashEpoch != -1 {
				d.status = "terminated"
				d.terminationReason = "entered on-chain final-slashed state"
			} else if d.State.SectorStartEpoch > 0 {
				d.sectorStart = &d.State.SectorStartEpoch
				d.status = "active"
			} else if d.Proposal.StartEpoch+filbuiltin.EpochsInDay < curTipset.Height() { // FIXME replace with DealUpdatesInterval
				// if things are that late: they are never going to make it
				d.status = "terminated"
				d.terminationReason = "containing sector missed expected sealing epoch"
			}

			// because of how we account for datacap, the in-db value must reflect everything not-yet-activated
			if dcap, known := tenantClientDatacap[d.Proposal.Client]; known && d.Proposal.VerifiedDeal && d.status == "published" {
				if dcap == nil {
					return xerrors.Errorf("client %s does not seem to have datacap yet published fil+ deal %d", d.Proposal.Client, d.dealID)
				}
				nv := filbig.Add(*dcap, filbig.NewInt(int64(d.Proposal.PieceSize)))
				tenantClientDatacap[d.Proposal.Client] = &nv
			}

			dealCountsByState[d.status]++
			if d.prevState == nil {
				if d.status == "terminated" {
					dealCountsByState["terminatedNewDirect"]++
				} else if d.status == "active" {
					dealCountsByState["activeNewDirect"]++
				} else {
					dealCountsByState["publishedNew"]++
				}
				toUpsert = append(toUpsert, &d)
			} else if d.status != d.prevState.status {
				dealCountsByState[d.status+"New"]++
				toUpsert = append(toUpsert, &d)
			}
		}

		// fill in some blanks
		for _, d := range toUpsert {

			if d.Proposal.Label.IsBytes() {
				d.label, _ = d.Proposal.Label.ToBytes()
			} else if d.Proposal.Label.IsString() {
				ls, _ := d.Proposal.Label.ToString()
				d.label = []byte(ls)
			} else {
				return xerrors.New("this should not happen...")
			}

			if lc, err := cid.Parse(string(d.label)); err == nil {
				if s := lc.String(); s != "" {
					d.decodedLabel = &s
				}
			}

			d.metaJSONB, err = json.Marshal(
				struct {
					TermReason string `json:"termination_reason,omitempty"`
				}{TermReason: d.terminationReason},
			)
			if err != nil {
				return cmn.WrErr(err)
			}

			d.clientID, err = fil.ParseActorString(d.Proposal.Client.String())
			if err != nil {
				return cmn.WrErr(err)
			}
			d.providerID, err = fil.ParseActorString(d.Proposal.Provider.String())
			if err != nil {
				return cmn.WrErr(err)
			}

			if bits.OnesCount64(uint64(d.Proposal.PieceSize)) != 1 {
				return xerrors.Errorf("deal %d size for is not a power of 2", d.Proposal.PieceSize)
			}
			d.pieceLog2Size = uint8(bits.TrailingZeros64(uint64(d.Proposal.PieceSize)))
		}

		toFail := make([]int64, 0, len(initialDbDeals))
		// whatever remains here is gone from the state entirely
		for dID, d := range initialDbDeals {
			dealCountsByState["terminated"]++
			if d.status != "terminated" {
				dealCountsByState["terminatedNew"]++
				toFail = append(toFail, dID)
			}
		}

		log.Infof(
			"about to upsert %s modified deal states, and terminate %s no longer existing deals",
			humanize.Comma(int64(len(toUpsert))),
			humanize.Comma(int64(len(toFail))),
		)

		return db.BeginTxFunc(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {

			for _, d := range toUpsert {
				if err = tx.QueryRow(
					ctx,
					`
					INSERT INTO spd.published_deals
						( deal_id, client_id, provider_id, piece_cid, claimed_log2_size, label, decoded_label, is_filplus, status, published_deal_meta, start_epoch, end_epoch, sector_start_epoch )
						VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::JSONB, $11, $12, $13 )
					ON CONFLICT ( deal_id ) DO UPDATE SET
						status = EXCLUDED.status,
						published_deal_meta = spd.published_deals.published_deal_meta || EXCLUDED.published_deal_meta,
						sector_start_epoch = COALESCE( EXCLUDED.sector_start_epoch, spd.published_deals.sector_start_epoch )
					RETURNING piece_id
					`,
					d.dealID,
					d.clientID,
					d.providerID,
					d.Proposal.PieceCID,
					d.pieceLog2Size,
					d.label,
					d.decodedLabel,
					d.Proposal.VerifiedDeal,
					d.status,
					d.metaJSONB,
					d.Proposal.StartEpoch,
					d.Proposal.EndEpoch,
					d.sectorStart,
				).Scan(&d.pieceID); err != nil {
					return cmn.WrErr(err)
				}

				if d.status == "active" && (d.prevState == nil || d.prevState.status != "active") {
					if _, err := tx.Exec(
						ctx,
						`
						UPDATE spd.proposals
							SET activated_deal_id = $1
						WHERE
							proposal_failstamp = 0
								AND
							proposal_delivered IS NOT NULL
								AND
							activated_deal_id IS NULL
								AND
							piece_id = $2
								AND
							provider_id = $3
								AND
							client_id = $4
						`,
						d.dealID,
						d.pieceID,
						d.providerID,
						d.clientID,
					); err != nil {
						return cmn.WrErr(err)
					}
				}
			}

			// we may have some terminations ( no longer in the market state )
			if len(toFail) > 0 {
				if _, err = tx.Exec(
					ctx,
					`
					UPDATE spd.published_deals SET
						status = 'terminated',
						published_deal_meta = published_deal_meta || '{ "termination_reason":"deal no longer part of market-actor state" }'
					WHERE
						deal_id = ANY ( $1::BIGINT[] )
							AND
						status != 'terminated'
					`,
					toFail,
				); err != nil {
					return cmn.WrErr(err)
				}
			}

			// update datacap
			for c, d := range tenantClientDatacap {
				var di *int64
				if d != nil {
					v := d.Int64()
					di = &v
				}
				if _, err := tx.Exec(
					ctx,
					`
					UPDATE spd.clients SET
						client_meta = JSONB_SET( client_meta, '{ activatable_datacap }', TO_JSONB( $1::BIGINT ) )
					WHERE
						client_id = $2
					`,
					di,
					fil.MustParseActorString(c.String()),
				); err != nil {
					return cmn.WrErr(err)
				}
			}

			// anything that activated is obviously the correct size
			if _, err := tx.Exec(
				ctx,
				`
				UPDATE spd.pieces SET piece_meta = piece_meta || '{ "size_proven_correct":true }',
						piece_log2_size = active.proven_log2_size
					FROM (
						SELECT pd.piece_id, pd.claimed_log2_size AS proven_log2_size
							FROM spd.published_deals pd, spd.pieces p
						WHERE
							( NOT COALESCE( (p.piece_meta->'size_proven_correct')::BOOL, false) )
								AND
							pd.piece_id = p.piece_id
								AND
							pd.status = 'active'
					) active
				WHERE
					( NOT COALESCE( (pieces.piece_meta->'size_proven_correct')::BOOL, false) )
						AND
					pieces.piece_id = active.piece_id
				`,
			); err != nil {
				return cmn.WrErr(err)
			}

			// clear out proposals that will never make it
			if _, err := tx.Exec(
				ctx,
				`
				UPDATE spd.proposals SET
					proposal_failstamp = spd.big_now(),
					proposal_meta = JSONB_SET(
						proposal_meta,
						'{ failure }',
						TO_JSONB( 'proposal DealStartEpoch missed without activation'::TEXT )
					)
				WHERE
					proposal_failstamp = 0
						AND
					activated_deal_id IS NULL
						AND
					start_epoch < $1
				`,
				curTipset.Height()-filbuiltin.EpochsInDay, // FIXME replace with DealUpdatesInterval
			); err != nil {
				return cmn.WrErr(err)
			}

			// clear out proposals that had an active deal which subsequently terminated
			if _, err := tx.Exec(
				ctx,
				`
				UPDATE spd.proposals SET
					activated_deal_id = NULL,
					proposal_failstamp = spd.big_now(),
					proposal_meta = JSONB_SET(
						proposal_meta,
						'{ failure }',
						TO_JSONB( 'sector containing deal was terminated'::TEXT )
					)
				WHERE
					activated_deal_id IN ( SELECT deal_id FROM spd.published_deals WHERE status = 'terminated' )
				`,
			); err != nil {
				return cmn.WrErr(err)
			}

			// clear out proposals that had an active deal which subsequently was deemed invalid
			if _, err := tx.Exec(
				ctx,
				`
				UPDATE spd.proposals SET
					proposal_failstamp = spd.big_now(),
					proposal_meta = JSONB_SET(
						proposal_meta,
						'{ failure }',
						TO_JSONB( 'deal declared invalid'::TEXT )
					)
				WHERE
					activated_deal_id IN ( SELECT deal_id FROM spd.invalidated_deals )
				`,
			); err != nil {
				return cmn.WrErr(err)
			}

			msJ, _ := json.Marshal(struct {
				Epoch  filabi.ChainEpoch `json:"epoch"`
				Tipset fil.LotusTSK      `json:"tipset"`
			}{
				Epoch:  curTipset.Height(),
				Tipset: curTipset.Key(),
			})

			if _, err := tx.Exec(
				ctx,
				`
				UPDATE spd.global SET metadata = JSONB_SET( metadata, '{ market_state }', $1 )
				`,
				msJ,
			); err != nil {
				return cmn.WrErr(err)
			}

			return refreshMatviews(ctx, tx)
		})
	},
}
