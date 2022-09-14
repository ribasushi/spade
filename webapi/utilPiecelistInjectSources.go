package main

import (
	"context"
	"sync"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"golang.org/x/sync/errgroup"
)

type piecePointers map[int64]pieceSources

type pieceSources struct {
	sourcesPointer      *[]types.DataSource
	pieceCid            string
	HasSourcesFilActive bool
	HasSourcesHTTP      bool
}

func injectSources(ctx context.Context, toFill piecePointers) error {
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
		eg.Go(func() error { return injectActiveFilDAG(ctx, filSrcIDs, toFill, pieceLocks) })
	}

	if err := eg.Wait(); err != nil {
		return cmn.WrErr(err)
	}

	return nil
}

func injectActiveFilDAG(ctx context.Context, ids []int64, ptrs piecePointers, pieceLocks map[int64]*sync.Mutex) error {

	rows, err := cmn.Db.Query(
		ctx,
		`
		SELECT
				piece_id,
				deal_id,
				end_epoch,
				provider_id,
				is_filplus,
				proposal_label
			FROM egd.known_fildag_deals_ranked
		WHERE
			rank = 1
				AND
			piece_id = ANY ( $1 )
		ORDER BY
			piece_id,
			is_filplus DESC,
			end_epoch DESC,
			deal_id
		`,
		ids,
	)
	if err != nil {
		return cmn.WrErr(err)
	}
	defer rows.Close()

	var spID, pieceID int64
	for rows.Next() {
		var srcEntry types.FilSourceDAG
		var dealEndEpoch filabi.ChainEpoch

		if err := rows.Scan(&pieceID, &srcEntry.DealID, &dealEndEpoch, &spID, &srcEntry.IsFilplus, &srcEntry.OriginalPayloadCid); err != nil {
			return cmn.WrErr(err)
		}
		p := ptrs[pieceID]
		srcEntry.ProviderID = cmn.ActorID(spID).String()
		srcEntry.DealExpiration = cmn.MainnetTime(dealEndEpoch)
		if err := srcEntry.InitDerivedVals(p.pieceCid); err != nil {
			return cmn.WrErr(err)
		}
		pieceLocks[pieceID].Lock()
		*p.sourcesPointer = append(*p.sourcesPointer, &srcEntry)
		pieceLocks[pieceID].Unlock()
	}

	return cmn.WrErr(rows.Err())
}
