package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/cron/filtypes"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	lp2phost "github.com/libp2p/go-libp2p/core/host"
	lp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	filaddr "github.com/filecoin-project/go-address"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"

	lotusmarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	lotusapi "github.com/filecoin-project/lotus/api"
)

type proposalPending struct {
	ProposalUUID         uuid.UUID
	ProposalLabel        string
	ProposalPayload      filmarket.DealProposal
	ProposalSignature    filcrypto.Signature
	ProposalCid          string
	PeerID               *lp2ppeer.ID
	Multiaddrs           []string
	ProviderSupportsV120 bool
}
type proposalsPerSP map[filaddr.Address][]proposalPending

type runTotals struct {
	proposals       int
	uniqueProviders int
	deliveredLegacy *int32
	delivered120    *int32
	timedout        *int32
	failed          *int32
}

var (
	spProposalSleep int
	proposalTimeout int
	perSpTimeout    int
)
var proposePending = &cli.Command{
	Usage: "Propose pending deals to providers",
	Name:  "propose-pending",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:        "sleep-between-proposals",
			Usage:       "Amount of seconds to wait between proposals to same SP",
			Value:       3,
			Destination: &spProposalSleep,
		},
		&cli.IntFlag{
			Name:        "proposal-timeout",
			Usage:       "Amount of seconds before aborting a specific proposal",
			Value:       30,
			Destination: &proposalTimeout,
		},
		&cli.IntFlag{
			Name:        "per-sp-timeout",
			Usage:       "Amount of seconds proposals for specific SP could take in total",
			Value:       270, // 4.5 mins
			Destination: &perSpTimeout,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context

		tot := runTotals{
			deliveredLegacy: new(int32),
			delivered120:    new(int32),
			timedout:        new(int32),
			failed:          new(int32),
		}
		defer func() {
			log.Infow("summary",
				"uniqueProviders", tot.uniqueProviders,
				"proposals", tot.proposals,
				"successfulV120", atomic.LoadInt32(tot.delivered120),
				"successfulLegacy", atomic.LoadInt32(tot.deliveredLegacy),
				"failed", atomic.LoadInt32(tot.failed),
				"timedout", atomic.LoadInt32(tot.timedout),
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
					p.proposal_label,
					( pi.info->'peer_info'->'libp2p_protocols'->$1 ) IS NOT NULL AS provider_supports_v120,
					pi.info->'peerid' AS peer_id,
					pi.info->'multiaddrs' AS multiaddrs
				FROM egd.proposals pr
				JOIN egd.pieces p USING ( piece_id )
				LEFT JOIN egd.providers_info pi USING ( provider_id )
			WHERE
				proposal_delivered IS NULL
					AND
				signature_obtained IS NOT NULL
					AND
				proposal_failstamp = 0
			ORDER BY entry_created
			`,
			filtypes.StorageProposalV120,
		); err != nil {
			return cmn.WrErr(err)
		}

		props := make(proposalsPerSP, 4)
		for _, p := range pending {

			if p.PeerID == nil || len(p.Multiaddrs) == 0 {
				if _, err := cmn.Db.Exec(
					context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
					`
					UPDATE egd.proposals SET
						proposal_failstamp = egd.big_now(),
						proposal_meta = JSONB_SET( proposal_meta, '{ failure }', TO_JSONB( 'provider not dialable: insufficient information published on chain'::TEXT ) )
					WHERE
						proposal_uuid = $1
					`,
					p.ProposalUUID,
				); err != nil {
					return cmn.WrErr(err)
				}
				continue
			}

			props[p.ProposalPayload.Provider] = append(props[p.ProposalPayload.Provider], p)
			tot.proposals++
		}
		tot.uniqueProviders = len(props)

		eg, ctx := errgroup.WithContext(ctx)
		for sp := range props {
			sp := sp
			eg.Go(func() error { return proposeToSp(ctx, props[sp], tot) })
		}
		return eg.Wait()
	},
}

func proposeToSp(ctx context.Context, props []proposalPending, tot runTotals) error {

	sp := props[0].ProposalPayload.Provider
	propType := "legacy"
	if props[0].ProviderSupportsV120 {
		propType = "v120"
	}

	dealCount := len(props)
	jobDesc := fmt.Sprintf("proposing %d deals to %s(%s)", dealCount, sp, propType)
	var delivered, failed, timedout int
	log.Info("START " + jobDesc)
	t0 := time.Now()
	defer func() {
		log.Infof(
			"END %s, out of %d proposals: %d succeeded, %d failed, %d timed out, took %s",
			jobDesc,
			dealCount,
			delivered, failed, timedout,
			time.Since(t0).String(),
		)
	}()

	// some SPs take *FOREVER* to respond ( 40+ seconds )
	// Cap processing, so that the rest of the queue isn't held up
	// ( they will restart from where they left off on next round )
	ctx, cancel := context.WithDeadline(ctx, t0.Add(time.Duration(perSpTimeout)*time.Second))
	defer cancel()

	var nodeHost lp2phost.Host
	var dialTookMsecs *int64
	var localPeerid *string
	for i, p := range props {

		// wait a bit between deliveries
		if i != 0 {
			select {
			case <-ctx.Done():
				return nil // timeout is not an error
			case <-time.After(time.Duration(spProposalSleep) * time.Second):
			}
		}

		var propCidStr *string
		var callErr error
		var proposingTookMsecs *int64

		if !p.ProviderSupportsV120 {

			var propCid *cid.Cid
			tCtx, tCtxCancel := context.WithTimeout(ctx, time.Duration(proposalTimeout)*time.Second)
			t1 := time.Now()
			propCid, callErr = cmn.LotusAPIHeavy.ClientStatelessDeal(
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
			pms := time.Since(t1).Milliseconds()
			proposingTookMsecs = &pms
			tCtxCancel()
			if propCid != nil {
				s := propCid.String()
				propCidStr = &s
			}
		} else {

			// connect on first iteration
			if nodeHost == nil {

				var err error
				nodeHost, _, err = newLp2pNode(time.Duration(proposalTimeout) * time.Second)
				if err != nil {
					return cmn.WrErr(err)
				}

				defer func() {
					if err := nodeHost.Close(); err != nil {
						log.Warnf("unexpected error shutting down node %s: %s", nodeHost.ID().String(), err)
					}
				}()

				lpid := nodeHost.ID().String()
				localPeerid = &lpid

				pTag := "proposing"
				nodeHost.ConnManager().Protect(*p.PeerID, pTag)
				defer nodeHost.ConnManager().Unprotect(*p.PeerID, pTag)

				addrs := make([]multiaddr.Multiaddr, len(p.Multiaddrs))
				for i := range p.Multiaddrs {
					addrs[i] = multiaddr.StringCast(p.Multiaddrs[i])
				}
				t1 := time.Now()
				callErr = nodeHost.Connect(ctx, lp2ppeer.AddrInfo{
					ID:    *p.PeerID,
					Addrs: addrs,
				})
				dms := time.Since(t1).Milliseconds()
				dialTookMsecs = &dms
			}

			if callErr == nil {
				var resp filtypes.StorageProposalV120Response
				tCtx, tCtxCancel := context.WithTimeout(ctx, time.Duration(proposalTimeout)*time.Second)
				t1 := time.Now()
				callErr = lp2pRPC(
					tCtx,
					nodeHost,
					*p.PeerID,
					filtypes.StorageProposalV120,
					&filtypes.StorageProposalV120Params{
						DealUUID:     p.ProposalUUID,
						IsOffline:    true,
						DealDataRoot: p.ProposalPayload.PieceCID,
						ClientDealProposal: filmarket.ClientDealProposal{
							Proposal:        p.ProposalPayload,
							ClientSignature: p.ProposalSignature,
						},
					},
					&resp,
				)
				pms := time.Since(t1).Milliseconds()
				proposingTookMsecs = &pms
				tCtxCancel()
				if callErr == nil && !resp.Accepted {
					callErr = xerrors.New(resp.Message)
				}
			}
		}

		// we did it!
		if callErr == nil {

			delivered++
			if p.ProviderSupportsV120 {
				atomic.AddInt32(tot.delivered120, 1)
			} else {
				atomic.AddInt32(tot.deliveredLegacy, 1)
			}

			if propCidStr != nil && p.ProposalCid != *propCidStr {
				// this is a soft-warning, not catastrophic (see below)
				log.Warnf("proposal CID mismatch on success: expected %s got %s", p.ProposalCid, *propCidStr)
			}

			if _, err := cmn.Db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE egd.proposals SET
					proposal_delivered = NOW(),
					proposal_meta = JSONB_STRIP_NULLS(
						JSONB_SET(
							JSONB_SET(
								JSONB_SET(
									-- FIXME - remove when v120 is default
									-- we need to re-write the signed_proposal_cid, as Lotus is known to mangle things for us at times
									JSONB_SET(
										proposal_meta,
										'{ signed_proposal_cid }',
										TO_JSONB( COALESCE( $2::TEXT, proposal_meta->>'signed_proposal_cid' ) )
									),
									'{ dialing_peerid }',
									COALESCE( TO_JSONB( $3::TEXT ), 'null'::JSONB )
								),
								'{ dial_took_msecs }',
								COALESCE( TO_JSONB( $4::BIGINT ), 'null'::JSONB )
							),
							'{ proposal_took_msecs }',
							COALESCE( TO_JSONB( $5::BIGINT ), 'null'::JSONB )
						)
					)
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
				propCidStr,
				localPeerid,
				dialTookMsecs,
				proposingTookMsecs,
			); err != nil {
				return cmn.WrErr(err)
			}
		} else {
			log.Error(callErr)

			didTimeout := errors.Is(callErr, context.DeadlineExceeded)
			if didTimeout {
				timedout++
				atomic.AddInt32(tot.timedout, 1)
			} else {
				failed++
				atomic.AddInt32(tot.failed, 1)
			}

			if _, err := cmn.Db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE egd.proposals SET
					proposal_failstamp = egd.big_now(),
					proposal_meta = JSONB_STRIP_NULLS(
						JSONB_SET(
							JSONB_SET(
								JSONB_SET(
									JSONB_SET(
										proposal_meta,
										'{ failure }',
										TO_JSONB( $2::TEXT )
									),
									'{ dialing_peerid }',
									COALESCE( TO_JSONB( $3::TEXT ), 'null'::JSONB )
								),
								'{ dial_took_msecs }',
								COALESCE( TO_JSONB( $4::BIGINT ), 'null'::JSONB )
							),
							'{ proposal_took_msecs }',
							COALESCE( TO_JSONB( $5::BIGINT ), 'null'::JSONB )
						)
					)
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
				callErr.Error(),
				localPeerid,
				dialTookMsecs,
				proposingTookMsecs,
			); err != nil {
				return cmn.WrErr(err)
			}

			// in case of a timeout or connection failure: bail after failing just one proposal, retry next time
			if (p.ProviderSupportsV120 && nodeHost == nil) ||
				didTimeout {
				return nil
			}
		}
	}

	return nil
}
