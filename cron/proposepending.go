package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
	filaddr "github.com/filecoin-project/go-address"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/context"
)

var proposePending = &cli.Command{
	Usage: "Propose pending deals to providers",
	Name:  "propose-pending",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx := cctx.Context

		totalDone := new(int32)
		totalFails := new(int32)
		props := make(map[filaddr.Address][]*lotusapi.StartDealParams, 16)
		defer func() {
			log.Infow("summary",
				"uniqueProviders", len(props),
				"successful", atomic.LoadInt32(totalDone),
				"failed", atomic.LoadInt32(totalFails),
			)
		}()

		rows, err := common.Db.Query(
			ctx,
			`
			SELECT
					dealstart_payload
				FROM proposals
			WHERE
				proposal_success_cid IS NULL
					AND
				proposal_failstamp = 0
			ORDER BY entry_created, piece_cid
			`,
		)
		if err != nil {
			return err
		}

		for rows.Next() {
			var j []byte
			if err = rows.Scan(&j); err != nil {
				return err
			}

			p := new(lotusapi.StartDealParams)
			if err = json.Unmarshal(j, &p); err != nil {
				return err
			}

			if _, exists := props[p.Miner]; !exists {
				props[p.Miner] = make([]*lotusapi.StartDealParams, 0, 128)
			}
			props[p.Miner] = append(props[p.Miner], p)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		rows.Close()

		if len(props) == 0 {
			return nil
		}

		var wg sync.WaitGroup
		for spID, spProps := range props {

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

				for _, p := range spProps {

					// some SPs take *FOREVER* to respond ( 40+ seconds )
					// Cut them off early, so that the rest of the queue isn't held up
					if time.Since(t0) > 4*time.Minute {
						log.Warnf("Process of %s is taking too long, aborting", jobDesc)
						return
					}

					var propCid *cid.Cid
					var apiErr, dbErr error
					{
						tCtx, tCtxCancel := context.WithTimeout(ctx, 90*time.Second)
						propCid, apiErr = common.LotusAPI.ClientStatelessDeal(
							tCtx,
							p,
						)
						tCtxCancel()
					}

					if apiErr != nil {
						countFailed++
						atomic.AddInt32(totalFails, 1)
						log.Warnf("While %s, encountered failure: %s", jobDesc, apiErr.Error())
						_, dbErr = common.Db.Exec(
							ctx,
							`
							UPDATE proposals SET
								proposal_failstamp = $1,
								meta = JSONB_SET(
									COALESCE( meta, '{}'),
									'{ failure }',
									TO_JSONB($2)
								)
							WHERE
								provider_id = $3
									AND
								piece_cid = $4
									AND
								proposal_failstamp = 0
									AND
								proposal_success_cid IS NULL
							`,
							time.Now().UnixNano(),
							apiErr.Error(),
							p.Miner.String(),
							p.Data.PieceCid.String(),
						)
					} else {
						countProposed++
						atomic.AddInt32(totalDone, 1)
						_, dbErr = common.Db.Exec(
							ctx,
							`
							UPDATE proposals
								SET proposal_success_cid = $1
							WHERE
								provider_id = $2
									AND
								piece_cid = $3
									AND
								proposal_failstamp = 0
									AND
								proposal_success_cid IS NULL
							`,
							propCid.String(),
							p.Miner.String(),
							p.Data.PieceCid.String(),
						)
					}

					if dbErr != nil {
						log.Warnw("unexpected error updating proposal", "proposal", p, "error", dbErr.Error())
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
					}
				}
			}()
		}

		wg.Wait()

		return nil
	},
}
