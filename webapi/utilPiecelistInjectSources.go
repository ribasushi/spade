package main

import (
	"context"
	"fmt"
	"sync"

	apitypes "github.com/data-preservation-programs/go-spade-apitypes"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/spade/internal/app"
	"golang.org/x/sync/errgroup"
)

type piecePointers map[int64]pieceSources

type pieceSources struct {
	sourcesPointer      *[]apitypes.DataSource
	pieceCid            string
	HasSourcesFilActive bool
	HasSourcesHTTP      bool
}

func injectSources(ctx context.Context, toFill piecePointers, onlyOrg int16) error {
	if len(toFill) == 0 {
		return nil
	}

	var eg *errgroup.Group
	eg, ctx = errgroup.WithContext(ctx)

	pieceLocks := make(map[int64]*sync.Mutex, 128<<10)
	filSrcIDs := make([]int64, 0, len(toFill))
	for pieceID, p := range toFill {
		var useLock sync.Mutex
		if p.HasSourcesFilActive {
			filSrcIDs = append(filSrcIDs, pieceID)
			pieceLocks[pieceID] = &useLock
		}
	}

	if len(filSrcIDs) > 0 {
		eg.Go(func() error { return injectActiveFilDAG(ctx, filSrcIDs, toFill, onlyOrg, pieceLocks) })
	}

	if err := eg.Wait(); err != nil {
		return cmn.WrErr(err)
	}

	return nil
}

func injectActiveFilDAG(ctx context.Context, ids []int64, ptrs piecePointers, onlyOrg int16, pieceLocks map[int64]*sync.Mutex) error {

	var orgCond string
	if onlyOrg != 0 {
		orgCond = fmt.Sprintf(
			` AND p.org_id = %d`,
			onlyOrg,
		)
	}

	rows, err := app.GetGlobalCtx(ctx).Db[app.DbMain].Query(
		ctx,
		fmt.Sprintf(
			`
			SELECT
					piece_id,
					deal_id,
					end_epoch,
					provider_id,
					is_filplus,
					proposal_label
				FROM spd.known_fildag_deals_ranked kfdr
				JOIN spd.providers p USING ( provider_id )
			WHERE
				rank = 1
					AND
				piece_id = ANY ( $1 )%s
			ORDER BY
				piece_id,
				is_filplus DESC,
				end_epoch DESC,
				deal_id
			`,
			orgCond,
		),
		ids,
	)
	if err != nil {
		return cmn.WrErr(err)
	}
	defer rows.Close()

	var spID, pieceID int64
	for rows.Next() {
		var srcEntry apitypes.FilSourceDAG
		var dealEndEpoch filabi.ChainEpoch

		if err := rows.Scan(&pieceID, &srcEntry.DealID, &dealEndEpoch, &spID, &srcEntry.IsFilplus, &srcEntry.OriginalPayloadCid); err != nil {
			return cmn.WrErr(err)
		}
		p := ptrs[pieceID]
		srcEntry.ProviderID = fil.ActorID(spID).String()
		srcEntry.DealExpiration = fil.MainnetTime(dealEndEpoch)
		if err := srcEntry.InitDerivedVals(p.pieceCid); err != nil {
			return cmn.WrErr(err)
		}
		pieceLocks[pieceID].Lock()
		*p.sourcesPointer = append(*p.sourcesPointer, &srcEntry)
		pieceLocks[pieceID].Unlock()
	}

	return cmn.WrErr(rows.Err())
}
