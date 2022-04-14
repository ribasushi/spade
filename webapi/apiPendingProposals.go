package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filaddr "github.com/filecoin-project/go-address"
	"github.com/labstack/echo/v4"
)

func apiListPendingProposals(c echo.Context) error {

	sp, err := filaddr.NewFromString(c.Response().Header().Get("X-FIL-SPID"))
	if err != nil {
		return err
	}

	ctx := c.Request().Context()

	r := types.ResponsePendingProposals{
		PendingProposals: make([]types.DealProposal, 0, 128),
	}

	var isActive bool
	var customMaxOutstandingGiB *int64
	err = common.Db.QueryRow(
		ctx,
		`
		SELECT
			(
				SELECT is_active FROM providers WHERE provider_id = $1
			),
			COALESCE(
				(
					SELECT SUM ( p.padded_size )
						FROM pieces p
						JOIN proposals pr USING ( piece_cid )
					WHERE
						pr.proposal_failstamp = 0
							AND
						pr.activated_deal_id IS NULL
							AND
						pr.provider_id = $1
				),
				0
			) AS cur_outstanding_bytes,

			( SELECT (meta->>'max_outstanding_GiB')::INTEGER FROM providers WHERE provider_id = $1 ) AS max_outstanding_gib
		`,
		sp.String(),
	).Scan(&isActive, &r.CurOutstandingBytes, &customMaxOutstandingGiB)
	if err != nil {
		return err
	}

	maxBytes := common.MaxOutstandingGiB
	if customMaxOutstandingGiB != nil && *customMaxOutstandingGiB > common.MaxOutstandingGiB {
		maxBytes = *customMaxOutstandingGiB
	}
	maxBytes <<= 30

	rows, err := common.Db.Query(
		ctx,
		`
		SELECT
				pr.proposal_success_cid,
				pr.proposal_failstamp,
				pr.meta->>'failure',
				pr.start_by,
				(pr.dealstart_payload->'DealStartEpoch')::BIGINT AS start_epoch,
				p.piece_cid,
				p.padded_size,
				pl.payload_cid
			FROM proposals pr
			JOIN pieces p USING ( piece_cid )
			JOIN payloads pl USING ( piece_cid )
		WHERE
			pr.provider_id = $1
				AND
			( pr.start_by - NOW() ) > '1 hour'::INTERVAL
				AND
			pr.activated_deal_id is NULL
		`,
		sp.String(),
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var totalSize, countPendingProposals int64
	fails := make(map[int64]types.ProposalFailure, 128)
	for rows.Next() {
		var prop types.DealProposal
		var dCid, failure *string
		var failstamp int64
		if err = rows.Scan(&dCid, &failstamp, &failure, &prop.StartTime, &prop.StartEpoch, &prop.PieceCid, &prop.PieceSize, &prop.RootCid); err != nil {
			return err
		}

		if failstamp > 0 {
			t := time.Unix(0, failstamp)
			if time.Since(t) < 24*time.Hour {
				fails[failstamp] = types.ProposalFailure{
					Tstamp:   t,
					Err:      *failure,
					PieceCid: prop.PieceCid,
					RootCid:  prop.RootCid,
				}
			}
			continue
		}

		totalSize += prop.PieceSize

		if dCid == nil {
			countPendingProposals++
		} else {
			prop.DealCid = *dCid
			prop.HoursRemaining = int(time.Until(prop.StartTime).Truncate(time.Hour).Hours())
			prop.ImportCMD = fmt.Sprintf("lotus-miner storage-deals import-data %s %s__%s.car",
				*dCid,
				common.TrimCidString(prop.PieceCid),
				common.TrimCidString(prop.RootCid),
			)
			r.PendingProposals = append(r.PendingProposals, prop)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()

	sort.Slice(r.PendingProposals, func(i, j int) bool {
		pi, pj := r.PendingProposals[i], r.PendingProposals[j]
		ti, tj := time.Until(pi.StartTime).Truncate(time.Hour), time.Until(pj.StartTime).Truncate(time.Hour)
		switch {
		case pi.PieceSize != pj.PieceSize:
			return pi.PieceSize < pj.PieceSize
		case ti != tj:
			return ti < tj
		default:
			return pi.PieceCid != pj.PieceCid
		}
	})

	msg := strings.Join([]string{
		"This is an overview of deals recently proposed to SP " + sp.String(),
		fmt.Sprintf(
			"There currently are %d proposals to send out, and %d successful proposals totalling %0.2f GiB awaiting sealing.",
			countPendingProposals,
			len(r.PendingProposals),
			float64(r.CurOutstandingBytes)/(1<<30),
		),
	}, "\n")

	if isActive {
		msg += fmt.Sprintf(
			"\nYou can request an additional %0.2f GiB of proposals before exhausting your in-flight quota.",
			float64(maxBytes-r.CurOutstandingBytes)/(1<<30),
		)
		r.MaxOutstandingBytes = &maxBytes
	}

	if len(fails) > 0 {
		msg += fmt.Sprintf("\n\nIn the past 24h there were %d proposal errors, shown below.", len(fails))

		r.RecentFailures = make([]types.ProposalFailure, 0, len(fails))
		for _, f := range fails {
			r.RecentFailures = append(r.RecentFailures, f)
		}
		sort.Slice(r.RecentFailures, func(i, j int) bool {
			return r.RecentFailures[j].Tstamp.Before(r.RecentFailures[i].Tstamp)
		})
	}

	return retPayloadAnnotated(
		c,
		200,
		r,
		msg,
	)
}
