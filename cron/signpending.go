package main

import (
	"sync/atomic"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	filaddr "github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/urfave/cli/v2"
)

type signTotals struct {
	signed  *int32
	timeout *int32
	failed  *int32
}

var signPending = &cli.Command{
	Usage: "Sign pending deal proposals",
	Name:  "sign-pending",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx := cctx.Context

		totals := signTotals{
			signed:  new(int32),
			failed:  new(int32),
			timeout: new(int32),
		}
		wallets := make(map[filaddr.Address]struct{}, 16)
		defer func() {
			log.Infow("summary",
				"uniqueWallets", len(wallets),
				"successful", atomic.LoadInt32(totals.signed),
				"failed", atomic.LoadInt32(totals.failed),
			)
		}()

		type signaturePending struct {
			ProposalUUID    string
			ProposalPayload filmarket.DealProposal
		}

		pending := make([]signaturePending, 0, 128)
		if err := pgxscan.Select(
			ctx,
			cmn.Db,
			&pending,
			`
			SELECT
					pr.proposal_uuid,
					pr.proposal_meta->'filmarket_proposal' AS proposal_payload
				FROM egd.proposals pr
			WHERE
				signature_obtained IS NULL
					AND
				proposal_failstamp = 0
			`,
		); err != nil {
			return cmn.WrErr(err)
		}

		for _, p := range pending {
			wallets[p.ProposalPayload.Client] = struct{}{}

			raw, err := cborutil.Dump(&p.ProposalPayload)
			if err != nil {
				return cmn.WrErr(err)
			}

			sig, err := cmn.LotusAPIHeavy.WalletSign(ctx, p.ProposalPayload.Client, raw)
			if err != nil {
				return cmn.WrErr(err)
			}

			propNode, err := cborutil.AsIpld(&filmarket.ClientDealProposal{
				Proposal:        p.ProposalPayload,
				ClientSignature: *sig,
			})
			if err != nil {
				return cmn.WrErr(err)
			}

			if _, err := cmn.Db.Exec(
				ctx,
				`
				UPDATE egd.proposals SET
					signature_obtained = NOW(),
					proposal_meta = JSONB_SET(
						JSONB_SET(
							proposal_meta,
							'{ signature }',
							$1
						),
						'{ signed_proposal_cid }',
						TO_JSONB( $2::TEXT )
					)
				WHERE proposal_uuid = $3
				`,
				sig,
				propNode.Cid().String(),
				p.ProposalUUID,
			); err != nil {
				return cmn.WrErr(err)
			}

			atomic.AddInt32(totals.signed, 1)
		}

		return nil
	},
}
