package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/uuid"
	"github.com/multiformats/go-multiaddr"
	"github.com/ribasushi/go-toolbox-interplanetary/lp2p"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"github.com/ribasushi/spade/internal/app"
	"github.com/ribasushi/spade/internal/filtypes"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	filaddr "github.com/filecoin-project/go-address"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
)

type proposalPending struct {
	ProposalUUID      uuid.UUID
	ProposalLabel     string
	ProposalPayload   filmarket.DealProposal
	ProposalSignature filcrypto.Signature
	ProposalCid       string
	PeerID            *lp2p.PeerID
	Multiaddrs        []string
}
type proposalsPerSP map[filaddr.Address][]proposalPending

type runTotals struct {
	proposals       int
	uniqueProviders int
	delivered120    *int32
	timedout        *int32
	failed          *int32
}

var (
	spProposalSleep int
	proposalTimeout int
	perSpTimeout    int
)
var proposePending = &ufcli.Command{
	Usage: "Propose pending deals to providers",
	Name:  "propose-pending",
	Flags: []ufcli.Flag{
		&ufcli.IntFlag{
			Name:        "sleep-between-proposals",
			Usage:       "Amount of seconds to wait between proposals to same SP",
			Value:       3,
			Destination: &spProposalSleep,
		},
		&ufcli.IntFlag{
			Name:        "proposal-timeout",
			Usage:       "Amount of seconds before aborting a specific proposal",
			Value:       30,
			Destination: &proposalTimeout,
		},
		&ufcli.IntFlag{
			Name:        "per-sp-timeout",
			Usage:       "Amount of seconds proposals for specific SP could take in total",
			Value:       270, // 4.5 mins
			Destination: &perSpTimeout,
		},
	},
	Action: func(cctx *ufcli.Context) error {
		ctx, log, db, _ := app.UnpackCtx(cctx.Context)

		tot := runTotals{
			delivered120: new(int32),
			timedout:     new(int32),
			failed:       new(int32),
		}
		defer func() {
			log.Infow("summary",
				"uniqueProviders", tot.uniqueProviders,
				"proposals", tot.proposals,
				"successfulV120", atomic.LoadInt32(tot.delivered120),
				"failed", atomic.LoadInt32(tot.failed),
				"timedout", atomic.LoadInt32(tot.timedout),
			)
		}()

		pending := make([]proposalPending, 0, 2048)
		if err := pgxscan.Select(
			ctx,
			db,
			&pending,
			`
			SELECT
					pr.proposal_uuid,
					pr.proposal_meta->'filmarket_proposal' AS proposal_payload,
					pr.proposal_meta->'signature' AS proposal_signature,
					pr.proposal_meta->>'signed_proposal_cid' AS proposal_cid,
					p.proposal_label,
					pi.info->'peerid' AS peer_id,
					pi.info->'multiaddrs' AS multiaddrs
				FROM spd.proposals pr
				JOIN spd.pieces p USING ( piece_id )
				LEFT JOIN spd.providers_info pi USING ( provider_id )
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

		props := make(proposalsPerSP, 4)
		for _, p := range pending {

			if p.PeerID == nil || len(p.Multiaddrs) == 0 {
				if _, err := db.Exec(
					context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
					`
					UPDATE spd.proposals SET
						proposal_failstamp = spd.big_now(),
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
	if len(props) == 0 {
		return nil
	}

	ctx, log, db, _ := app.UnpackCtx(ctx)
	sp := props[0].ProposalPayload.Provider

	dealCount := len(props)
	jobDesc := fmt.Sprintf("proposing %d deals to %s", dealCount, sp)
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

	// do everything in the same loop, even the lp2p dial, so that we can
	// reuse the db-update code either way
	var nodeHost lp2p.Host
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

		var proposalLoopErr error

		// connect if needed
		if nodeHost == nil {

			var err error
			nodeHost, _, err = lp2p.NewPlainNodeTCP(time.Duration(proposalTimeout) * time.Second)
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
			proposalLoopErr = nodeHost.Connect(ctx, lp2p.AddrInfo{
				ID:    *p.PeerID,
				Addrs: addrs,
			})
			dms := time.Since(t1).Milliseconds()
			dialTookMsecs = &dms
		}

		var proposingTookMsecs *int64
		if proposalLoopErr == nil {
			var resp filtypes.StorageProposalV120Response
			tCtx, tCtxCancel := context.WithTimeout(ctx, time.Duration(proposalTimeout)*time.Second)
			t1 := time.Now()
			proposalLoopErr = lp2p.DoCborRPC(
				tCtx,
				nodeHost,
				*p.PeerID,
				filtypes.StorageProposalV120,
				&filtypes.StorageProposalV12xParams{
					IsOffline:          true, // not negotiable: out-of-band-transfers forever
					DealUUID:           p.ProposalUUID,
					RemoveUnsealedCopy: false, // potentially allow tenants to request different defaults
					SkipIPNIAnnounce:   false, // in the murky future
					ClientDealProposal: filmarket.ClientDealProposal{
						Proposal:        p.ProposalPayload,
						ClientSignature: p.ProposalSignature,
					},
					// there is no "DataRoot" - always set to the PieceCID itself as per
					// https://filecoinproject.slack.com/archives/C03AQ3QAUG1/p1662622159003079?thread_ts=1662552800.028649&cid=C03AQ3QAUG1
					DealDataRoot: p.ProposalPayload.PieceCID,
				},
				&resp,
			)
			pms := time.Since(t1).Milliseconds()
			proposingTookMsecs = &pms
			tCtxCancel()
			if proposalLoopErr == nil && !resp.Accepted {
				proposalLoopErr = xerrors.New(resp.Message)
			}
		}

		// set a few extra common parts
		if _, err := db.Exec(
			context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
			`
			UPDATE spd.proposals SET
				proposal_meta = JSONB_STRIP_NULLS(
					JSONB_SET(
						JSONB_SET(
							JSONB_SET(
								proposal_meta,
								'{ dialing_peerid }',
								COALESCE( TO_JSONB( $2::TEXT ), 'null'::JSONB )
							),
							'{ dial_took_msecs }',
							COALESCE( TO_JSONB( $3::BIGINT ), 'null'::JSONB )
						),
						'{ proposal_took_msecs }',
						COALESCE( TO_JSONB( $4::BIGINT ), 'null'::JSONB )
					)
				)
			WHERE
				proposal_uuid = $1
			`,
			p.ProposalUUID,
			localPeerid,
			dialTookMsecs,
			proposingTookMsecs,
		); err != nil {
			return cmn.WrErr(err)
		}

		// we did it!
		if proposalLoopErr == nil {

			delivered++
			atomic.AddInt32(tot.delivered120, 1)

			if _, err := db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE spd.proposals SET
					proposal_delivered = NOW()
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
			); err != nil {
				return cmn.WrErr(err)
			}
		} else {
			log.Error(proposalLoopErr)

			didTimeout := errors.Is(proposalLoopErr, context.DeadlineExceeded)
			if didTimeout {
				timedout++
				atomic.AddInt32(tot.timedout, 1)
			} else {
				failed++
				atomic.AddInt32(tot.failed, 1)
			}

			if _, err := db.Exec(
				context.Background(), // deliberate: even if outer context is cancelled we still need to write to DB
				`
				UPDATE spd.proposals SET
					proposal_failstamp = spd.big_now(),
					proposal_meta = JSONB_STRIP_NULLS(
						JSONB_SET(
							proposal_meta,
							'{ failure }',
							TO_JSONB( $2::TEXT )
						)
					)
				WHERE
					proposal_uuid = $1
				`,
				p.ProposalUUID,
				proposalLoopErr.Error(),
			); err != nil {
				return cmn.WrErr(err)
			}

			// in case of a timeout or connection failure: bail after failing just one proposal, retry next time
			if nodeHost == nil || didTimeout {
				return nil
			}
		}
	}

	return nil
}
