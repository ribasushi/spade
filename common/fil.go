package common

import (
	"context"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuild "github.com/filecoin-project/lotus/build"
	filactor "github.com/filecoin-project/lotus/chain/actors/builtin"
	filtypes "github.com/filecoin-project/lotus/chain/types"
	filactors "github.com/filecoin-project/specs-actors/actors/builtin"
	"golang.org/x/xerrors"
)

func MainnetTime(e filabi.ChainEpoch) time.Time { return time.Unix(int64(e)*30+FilGenesisUnix, 0) }

func WallTimeEpoch(t time.Time) filabi.ChainEpoch {
	return abi.ChainEpoch(t.Unix()-FilGenesisUnix) / filactor.EpochDurationSeconds
}

func LotusLookbackTipset(ctx context.Context) (*filtypes.TipSet, error) {
	latestHead, err := LotusAPI.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed getting chain head: %w", err)
	}

	wallUnix := time.Now().Unix()
	filUnix := int64(latestHead.Blocks()[0].Timestamp)

	if wallUnix < filUnix ||
		wallUnix > filUnix+int64(
			filbuild.PropagationDelaySecs+(ApiMaxTipsetsBehind*filactors.EpochDurationSeconds),
		) {
		return nil, xerrors.Errorf(
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
		return nil, xerrors.Errorf("determining target tipset %d epochs ago failed: %w", lotusLookbackEpochs, err)
	}

	return tipsetAtLookback, nil
}
