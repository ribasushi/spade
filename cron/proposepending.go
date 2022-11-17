package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	filaddr "github.com/filecoin-project/go-address"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"

	lotusmarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	lotusapi "github.com/filecoin-project/lotus/api"
)

type deliveryMethod int

const (
	lotusClient = deliveryMethod(iota)
)

type proposalTotals struct {
	uniqueProviders int
	delivered       *int32
	timeout         *int32
	failed          *int32
}
type proposalPending struct {
	ProposalUUID      uuid.UUID
	ProposalLabel     string
	ProposalPayload   filmarket.DealProposal
	ProposalSignature filcrypto.Signature
	ProposalCid       string
}
type proposalsPerSP map[filaddr.Address][]proposalPending

var proposePending = &cli.Command{
	Usage: "Propose pending deals to providers",
	Name:  "propose-pending",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx := cctx.Context

		totals := proposalTotals{
			delivered: new(int32),
			timeout:   new(int32),
			failed:    new(int32),
		}
		defer func() {
			log.Infow("summary",
				"uniqueProviders", totals.uniqueProviders,
				"successful", atomic.LoadInt32(totals.delivered),
				"failed", atomic.LoadInt32(totals.failed),
				"timeout", atomic.LoadInt32(totals.timeout),
			)
		}()

		pending := make([]proposalPending, 0, 2048)
		if err := pgxscan.Select(
			ctx,
			cmn.Db,
			&pending,
			`
			SELECT
					pr.proposal_uuid,
					pr.proposal_meta->'filmarket_proposal' AS proposal_payload,
					pr.proposal_meta->'signature' AS proposal_signature,
					pr.proposal_meta->>'signed_proposal_cid' AS proposal_cid,
					p.proposal_label
				FROM egd.proposals pr
				JOIN egd.providers prov USING ( provider_id )
				JOIN egd.pieces p USING ( piece_id )
			WHERE
				proposal_delivered IS NULL
					AND
				signature_obtained IS NOT NULL
					AND
				proposal_failstamp = 0
			ORDER BY entry_created
			`,
		); err != nil {
			return cmn.WrErr(err)
		}

		props := make(map[deliveryMethod]proposalsPerSP, 4)
		props[lotusClient] = make(proposalsPerSP, 16) // for now only lotusClient is supported
		for _, p := range pending {
			sp := p.ProposalPayload.Provider
			if _, exists := props[lotusClient][sp]; !exists {
				props[lotusClient][sp] = make([]proposalPending, 0, 128)
			}
			props[lotusClient][sp] = append(props[lotusClient][sp], p)
		}

		eg := new(errgroup.Group)
		if len(props[lotusClient]) > 0 {
			eg.Go(func() error { return proposeViaLotusDaemon(ctx, props[lotusClient], totals) })
		}
		return eg.Wait()
	},
}

func proposeViaLotusDaemon(ctx context.Context, psp proposalsPerSP, totals proposalTotals) error {

	var wg sync.WaitGroup
	for spID, spProps := range psp {

		jobDesc := fmt.Sprintf("proposing %d deals to %s", len(spProps), spID)
		spProps := spProps

		wg.Add(1)
		log.Info("START " + jobDesc)
		go func() {

			var countProposed, countFailed int
			t0 := time.Now()

			defer func() {
				log.Infof("END %s, %d succeeded, %d failed, took %s", jobDesc, countProposed, countFailed, time.Since(t0).String())
				wg.Done()
			}()

			for i, p := range spProps {

				if i != 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
					}
				}

				// some SPs take *FOREVER* to respond ( 40+ seconds )
				// Cut them off early, so that the rest of the queue isn't held up
				if time.Since(t0) > 4*time.Minute {
					log.Warnf("Process of %s is taking too long, aborting", jobDesc)
					return
				}

				var propCid *cid.Cid
				var apiErr error
				{
					tCtx, tCtxCancel := context.WithTimeout(ctx, 90*time.Second)
					propCid, apiErr = cmn.LotusAPIHeavy.ClientStatelessDeal(
						tCtx,
						&lotusapi.StartDealParams{
							DealStartEpoch:     p.ProposalPayload.StartEpoch,
							MinBlocksDuration:  uint64(p.ProposalPayload.EndEpoch - p.ProposalPayload.StartEpoch),
							FastRetrieval:      true,
							VerifiedDeal:       p.ProposalPayload.VerifiedDeal,
							Wallet:             p.ProposalPayload.Client,
							Miner:              p.ProposalPayload.Provider,
							EpochPrice:         p.ProposalPayload.StoragePricePerEpoch,
							ProviderCollateral: p.ProposalPayload.ProviderCollateral,
							Data: &lotusmarket.DataRef{
								TransferType: lotusmarket.TTManual,
								PieceCid:     &p.ProposalPayload.PieceCID,
								PieceSize:    p.ProposalPayload.PieceSize.Unpadded(),
								Root:         cid.MustParse(p.ProposalLabel),
							},
						},
					)
					tCtxCancel()
				}

				var dbErr error
				if apiErr != nil {
					countFailed++
					atomic.AddInt32(totals.failed, 1)
					log.Warnf("While %s, encountered failure: %s", jobDesc, apiErr.Error())
					_, dbErr = cmn.Db.Exec(
						ctx,
						`
						UPDATE egd.proposals SET
							proposal_failstamp = egd.big_now(),
							proposal_meta = JSONB_SET( proposal_meta, '{ failure }', TO_JSONB( $1::TEXT ) )
						WHERE
							proposal_uuid = $2
						`,
						apiErr.Error(),
						p.ProposalUUID,
					)
				} else {
					if p.ProposalCid != propCid.String() {
						// this is a soft-warning, not catastrophic (see below)
						log.Warnf("proposal CID mismatch on success: expected %s got %s", p.ProposalCid, propCid.String())
					}

					countProposed++
					atomic.AddInt32(totals.delivered, 1)
					_, dbErr = cmn.Db.Exec(
						ctx,
						`
						UPDATE egd.proposals SET
							proposal_delivered = NOW(),
							-- we need to re-write the signed_proposal_cid, as Lotus is known to mangle things for us at times
							proposal_meta = JSONB_SET(
								proposal_meta,
								'{ signed_proposal_cid }',
								TO_JSONB( $2::TEXT )
							)
						WHERE
							proposal_uuid = $1
						`,
						p.ProposalUUID,
						propCid.String(),
					)
				}
				if dbErr != nil {
					log.Errorw("unexpected error updating proposal", "proposal", p, "error", dbErr.Error())
				}
			}
		}()
	}

	wg.Wait()

	return nil
}
