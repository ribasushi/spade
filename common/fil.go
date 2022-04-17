package common

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	filabi "github.com/filecoin-project/go-state-types/abi"
	lotusbuild "github.com/filecoin-project/lotus/build"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	filactors "github.com/filecoin-project/specs-actors/actors/builtin"
)

func MainnetTime(e filabi.ChainEpoch) time.Time { return time.Unix(int64(e)*30+FilGenesisUnix, 0) } //nolint:revive

func WallTimeEpoch(t time.Time) filabi.ChainEpoch { //nolint:revive
	return abi.ChainEpoch(t.Unix()-FilGenesisUnix) / filactors.EpochDurationSeconds
}

func LotusLookbackTipset(ctx context.Context) (*lotustypes.TipSet, error) { //nolint:revive
	latestHead, err := LotusAPI.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting chain head: %w", err)
	}

	wallUnix := time.Now().Unix()
	filUnix := int64(latestHead.Blocks()[0].Timestamp)

	if wallUnix < filUnix ||
		wallUnix > filUnix+int64(
			lotusbuild.PropagationDelaySecs+(ApiMaxTipsetsBehind*filactors.EpochDurationSeconds),
		) {
		return nil, fmt.Errorf(
			"lotus API out of sync: chainHead reports unixtime %d (height: %d) while walltime is %d (delta: %s)",
			filUnix,
			latestHead.Height(),
			wallUnix,
			time.Second*time.Duration(wallUnix-filUnix),
		)
	}

	latestHeight := latestHead.Height()

	tipsetAtLookback, err := LotusAPI.ChainGetTipSetByHeight(ctx, latestHeight-filabi.ChainEpoch(lotusLookbackEpochs), latestHead.Key())
	if err != nil {
		return nil, fmt.Errorf("determining target tipset %d epochs ago failed: %w", lotusLookbackEpochs, err)
	}

	return tipsetAtLookback, nil
}
