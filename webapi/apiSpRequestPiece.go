package main

import (
	"context"
	"fmt"
	"math/bits"
	"net/http"
	"strings"
	"time"

	apitypes "github.com/data-preservation-programs/go-spade-apitypes"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	filmarket "github.com/filecoin-project/go-state-types/builtin/v9/market"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/georgysavva/scany/pgxscan"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v4"
	"github.com/labstack/echo/v4"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/spade/internal/app"
	"github.com/ribasushi/spade/internal/filtypes"
)

var v1UrlEnc = multibase.MustNewEncoder(multibase.Base64url)

func apiSpRequestPiece(c echo.Context) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	pCidArg := c.Param("pieceCID")
	pCid, err := cid.Parse(pCidArg)
	if err != nil {
		return retFail(c, apitypes.ErrInvalidRequest, "Requested PieceCid '%s' is not valid: %s", pCidArg, err)
	}
	if pCid.Prefix().Codec != cid.FilCommitmentUnsealed || pCid.Prefix().MhType != multihash.SHA2_256_TRUNC254_PADDED {
		return retFail(
			c,
			apitypes.ErrInvalidRequest,
			"Requested PieceCID '%s' does not have expected codec (%x) and multihash (%x)",
			pCid,
			cid.FilCommitmentUnsealed,
			multihash.SHA2_256_TRUNC254_PADDED,
		)
	}

	tenantID := int16(0) // 0 == any
	if c.QueryParams().Has("tenant") {
		tid, err := parseUIntQueryParam(c, "tenant", 1, 1<<15)
		if err != nil {
			return retFail(c, apitypes.ErrInvalidRequest, err.Error())
		}
		tenantID = int16(tid)
	}

	// check whether the provider has been polled
	if ctxMeta.spInfoLastPolled == nil ||
		ctxMeta.spInfoLastPolled.Before(time.Now().Add(-1*app.PolledSPInfoStaleAfterMinutes*time.Minute)) {
		return retFail(
			c,
			apitypes.ErrStorageProviderInfoTooOld,
			"Provider has not been dialed by the polling system recently: please try again in about a minute",
		)
	}

	// check whether dialable at all
	if ctxMeta.spInfo.PeerInfo == nil || len(ctxMeta.spInfo.PeerInfo.Protos) == 0 {
		return retFail(
			c,
			apitypes.ErrStorageProviderUndialable,
			strings.Join([]string{
				"It appears your provider can not be libp2p-dialed over the TCP transport.",
				"Please invoke the status endpoint for further details:",
				curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/status"),
			}, "\n"),
		)
	}

	// only boost
	if _, canV120 := ctxMeta.spInfo.PeerInfo.Protos[filtypes.StorageProposalV120]; !canV120 {
		return retFail(
			c,
			apitypes.ErrStorageProviderUnsupported,
			strings.Join([]string{
				"It appears your provider does not support %s.",
				"You must upgrade to Boost v1.5.1 or equivalent to use ♠️",
			}, "\n"),
			filtypes.StorageProposalV120,
		)
	}

	errCode, err := spIneligibleErr(ctx, ctxMeta.authedActorID)
	if err != nil {
		return cmn.WrErr(err)
	} else if errCode != 0 {
		return retFail(c, errCode, ineligibleSpMsg(ctxMeta.authedActorID))
	}

	return ctxMeta.Db[app.DbMain].BeginFunc(ctx, func(tx pgx.Tx) error {

		_, err = tx.Exec(
			ctx,
			requestPieceLockStatement,
		)
		if err != nil {
			return cmn.WrErr(err)
		}

		type tenantEligible struct {
			apitypes.TenantReplicationState
			IsExclusive         bool         `db:"exclusive_replication"`
			TenantClientID      *fil.ActorID `db:"client_id_to_use"`
			TenantClientAddress *string      `db:"client_address_to_use"`

			ProposalLabel string
			PieceID       int64

			PieceSizeBytes int64

			DealDurationDays       int16
			StartWithinHours       int16
			RecentlyUsedStartEpoch *int64

			TenantMeta []byte
		}

		tenantsEligible := make([]tenantEligible, 0, 8)

		if err := pgxscan.Select(
			ctx,
			tx,
			&tenantsEligible,
			`
			SELECT
					*
				FROM spd.piece_realtime_eligibility( $1, $2 )
			WHERE
				proposal_label IS NOT NULL
					AND
				( 0 = $3 OR tenant_id = $3)
			`,
			ctxMeta.authedActorID,
			pCid,
			tenantID,
		); err != nil {
			return cmn.WrErr(err)
		}

		if len(tenantsEligible) == 0 {
			return retFail(c, apitypes.ErrUnclaimedPieceCID, "Piece %s is not claimed by any selected tenant", pCid)
		}

		if tenantsEligible[0].PieceSizeBytes > 1<<ctxMeta.spInfo.SectorLog2Size {
			return retFail(c, apitypes.ErrOversizedPiece,
				"Piece %s weighing %d GiB is larger than the %d GiB sector size your SP supports",
				pCid,
				tenantsEligible[0].PieceSizeBytes>>30,
				1<<(ctxMeta.spInfo.SectorLog2Size-30),
			)
		}

		// count ineligibles, assemble actual return
		var countNoDataCap, countAlreadyDealt, countOverReplicated, countOverPending int
		var chosenTenant *tenantEligible
		resp := apitypes.ResponseDealRequest{
			ReplicationStates: make([]apitypes.TenantReplicationState, len(tenantsEligible)),
		}
		for i, te := range tenantsEligible {
			if te.TenantClientID != nil {
				s := te.TenantClientID.String()
				te.TenantReplicationState.TenantClient = &s
			}
			resp.ReplicationStates[i] = te.TenantReplicationState

			var invalidated bool

			if te.TenantClient == nil {
				countNoDataCap++
				invalidated = true
			}
			if te.DealAlreadyExists {
				countAlreadyDealt++
				invalidated = true
			}
			if te.Total >= te.MaxTotal ||
				te.InOrg >= te.MaxOrg ||
				te.InCity >= te.MaxCity ||
				te.InCountry >= te.MaxCountry ||
				te.InContinent >= te.MaxContinent {
				countOverReplicated++
				invalidated = true
			}
			if te.SpInFlightBytes+te.PieceSizeBytes > te.MaxInFlightBytes {
				countOverPending++
				invalidated = true
			}

			if !invalidated && chosenTenant == nil {
				chosenTenant = &te
			}
		}

		// handle "no takers" here, for ease of reading further down
		// this is slightly convoluted since we can have a "mixed error condition" - handled in the default:
		if chosenTenant == nil {

			switch len(tenantsEligible) {

			case countAlreadyDealt:
				return retPayloadAnnotated(c, http.StatusForbidden,
					apitypes.ErrProviderHasReplica,
					resp,
					"Provider already has proposed or active replica for %s according to all selected replication rules", pCid,
				)
			case countNoDataCap:
				return retPayloadAnnotated(c, http.StatusForbidden,
					apitypes.ErrTenantsOutOfDatacap,
					resp,
					"All selected tenants with claim to %s are out of DataCap 🙀", pCid,
				)

			case countOverReplicated:
				return retPayloadAnnotated(c, http.StatusForbidden,
					apitypes.ErrTooManyReplicas,
					resp,
					"Piece %s is over-replicated according to all selected replication rules", pCid,
				)

			case countOverPending:
				return retPayloadAnnotated(c, http.StatusForbidden,
					apitypes.ErrProviderAboveMaxInFlight,
					resp,
					"Provider has more proposals in-flight than permitted by selected tenant rules",
				)

			default:
				return retPayloadAnnotated(c, http.StatusForbidden,
					apitypes.ErrReplicationRulesViolation,
					resp,
					"None of the selected tenants would grant a deal for %s according to their individual rules", pCid,
				)
			}
		}

		//
		// Here, at the very end, is where we would make a tightly-timeboxed outbound call
		// to check for potential external eligibility criteria
		// Then either return ErrExternalReservationRefused or proceed below.
		//
		// We *DO* always check using our own replication rules first, and keep a lock for the duration
		// ( in order to maintain a uniform "decency floor" among our esteemed SPs ;)
		//

		// We got that far - let's do it!
		startEpoch := fil.WallTimeEpoch(time.Now().Add(
			time.Hour * time.Duration(chosenTenant.StartWithinHours),
		))
		if chosenTenant.RecentlyUsedStartEpoch != nil {
			startEpoch = filabi.ChainEpoch(*chosenTenant.RecentlyUsedStartEpoch)
		}

		// this is relatively expensive to do within the txn lock
		// however we cache it and call it exactly once per day, so we should be fine
		gbpce, err := providerCollateralEstimateGiB(
			ctx,
			// round the epoch down to a day boundary
			// we *must* work with startEpoch to produce identical retry-deals
			((startEpoch-
				app.FilDefaultLookback-
				(filbuiltin.EpochsInHour*
					filabi.ChainEpoch(chosenTenant.StartWithinHours)))/
				2880)*
				2880,
		)
		if err != nil {
			return cmn.WrErr(err)
		}

		// // FIXME - use the long form client to match what lotus does ( drop when switching away )
		// cl, err := filaddr.NewFromString(*chosenTenant.TenantClientAddress)
		// if err != nil {
		// 	return cmn.WrErr(err)
		// }

		l := chosenTenant.ProposalLabel
		if lc, err := cid.Parse(l); err == nil && lc.Version() == 1 {
			l = lc.Encode(v1UrlEnc)
		}
		encodedLabel, err := filmarket.NewLabelFromString(l)
		if err != nil {
			return cmn.WrErr(err)
		}

		prop := struct {
			ProposalV0 filmarket.DealProposal `json:"filmarket_proposal"`
		}{
			ProposalV0: filmarket.DealProposal{

				// Label is a *completely* arbitrary, client-chosen nonce to apply to the deal, can be a UTF8-string or []bytes
				// For the time being it is the v0/b32v1 cid of the "root" in question, obviously subject to change
				// Current max-size is https://github.com/filecoin-project/go-state-types/blob/v0.9.9/builtin/v9/market/policy.go#L29-L30
				Label: encodedLabel,

				// do not change under any circumstances: even when payments eventually happen, they will happen explicitly out of band
				// ( a notable exception here would be contract-listener style interactions, but that's way off )
				StoragePricePerEpoch: filbig.Zero(), // DO NOT CHANGE

				VerifiedDeal: true,
				PieceCID:     pCid,
				PieceSize:    filabi.PaddedPieceSize(chosenTenant.PieceSizeBytes),

				Provider: ctxMeta.authedActorID.AsFilAddr(),
				Client:   chosenTenant.TenantClientID.AsFilAddr(),

				StartEpoch: startEpoch,
				EndEpoch:   startEpoch + filabi.ChainEpoch(chosenTenant.DealDurationDays)*filbuiltin.EpochsInDay,

				ClientCollateral: filbig.Zero(),
				ProviderCollateral: filbig.Rsh(
					filbig.Mul(gbpce, filbig.NewInt(chosenTenant.PieceSizeBytes)),
					30,
				),
			},
		}

		if _, err := tx.Exec(
			ctx,
			`
			INSERT INTO spd.proposals
				( piece_id, provider_id, client_id, start_epoch, end_epoch, proxied_log2_size, proposal_meta )
			VALUES ( $1, $2, $3, $4, $5, $6, $7 )
			`,
			chosenTenant.PieceID,
			ctxMeta.authedActorID,
			*chosenTenant.TenantClientID,
			prop.ProposalV0.StartEpoch,
			prop.ProposalV0.EndEpoch,
			bits.TrailingZeros64(uint64(chosenTenant.PieceSizeBytes)),
			prop,
		); err != nil {
			return cmn.WrErr(err)
		}

		// we managed - bump the counts where applicable and return stats
		for i := range tenantsEligible {
			if tenantsEligible[i].IsExclusive && resp.ReplicationStates[i].TenantID != chosenTenant.TenantID {
				continue
			}

			resp.ReplicationStates[i].Total++
			resp.ReplicationStates[i].InOrg++
			resp.ReplicationStates[i].InCity++
			resp.ReplicationStates[i].InCountry++
			resp.ReplicationStates[i].InContinent++
			resp.ReplicationStates[i].DealAlreadyExists = true
			resp.ReplicationStates[i].SpInFlightBytes += chosenTenant.PieceSizeBytes
		}

		return retPayloadAnnotated(
			c,
			http.StatusOK,
			0,
			resp,
			strings.Join([]string{
				fmt.Sprintf("Deal queued for PieceCID %s", pCid),
				``,
				`In about 5 minutes check the pending list:`,
				" " + curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/pending_proposals"),
			}, "\n"),
		)
	})
}

var collateralCache, _ = lru.New[filabi.ChainEpoch, filbig.Int](128)

func providerCollateralEstimateGiB(ctx context.Context, sourceEpoch filabi.ChainEpoch) (filbig.Int, error) { //nolint:revive
	if pc, didFind := collateralCache.Get(sourceEpoch); didFind {
		return pc, nil
	}

	lapi := app.GetGlobalCtx(ctx).LotusAPI[app.FilHeavy]

	ts, err := lapi.ChainGetTipSetByHeight(ctx, sourceEpoch, lotustypes.EmptyTSK)
	if err != nil {
		return filbig.Zero(), cmn.WrErr(err)
	}

	collateralGiB, err := lapi.StateDealProviderCollateralBounds(ctx, filabi.PaddedPieceSize(1<<30), true, ts.Key())
	if err != nil {
		return filbig.Zero(), cmn.WrErr(err)
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

	collateralCache.Add(sourceEpoch, inflatedCollateralGiB)
	return inflatedCollateralGiB, nil
}
