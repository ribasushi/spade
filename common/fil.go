package cmn

import (
	"context"
	"fmt"
	"strconv"
	"time"

	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lotusbuild "github.com/filecoin-project/lotus/build"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/xerrors"
)

type ActorID uint64 //nolint:revive

func (a ActorID) String() string { return fmt.Sprintf("f0%d", a) }
func (a ActorID) AsFilAddr() filaddr.Address { //nolint:revive
	r, _ := filaddr.NewIDAddress(uint64(a))
	return r
}

func ParseActorString(s string) (ActorID, error) { //nolint:revive
	if len(s) < 3 || (s[:2] != "f0" && s[:2] != "t0") {
		return 0, xerrors.Errorf("input '%s' does not have expected prefix", s)
	}

	val, err := strconv.ParseUint(s[2:], 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("unable to parse value of input '%s': %w", s, err)
	}

	return ActorID(val), nil
}

func MustParseActorString(s string) ActorID { //nolint:revive
	a, err := ParseActorString(s)
	if err != nil {
		panic(fmt.Sprintf("unexpected error parsing '%s': %s", s, err))
	}
	return a
}

func MainnetTime(e filabi.ChainEpoch) time.Time { return time.Unix(int64(e)*30+FilGenesisUnix, 0) } //nolint:revive

func WallTimeEpoch(t time.Time) filabi.ChainEpoch { //nolint:revive
	return filabi.ChainEpoch(t.Unix()-FilGenesisUnix) / filbuiltin.EpochDurationSeconds
}

func DefaultLookbackTipset(ctx context.Context) (*lotustypes.TipSet, error) { //nolint:revive
	latestHead, err := LotusAPICurState.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed getting chain head: %w", err)
	}

	wallUnix := time.Now().Unix()
	filUnix := int64(latestHead.Blocks()[0].Timestamp)

	if wallUnix < filUnix-2 || // allow couple seconds clock-drift tolerance
		wallUnix > filUnix+int64(
			lotusbuild.PropagationDelaySecs+(ApiMaxTipsetsBehind*filbuiltin.EpochDurationSeconds),
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

	tipsetAtLookback, err := LotusAPICurState.ChainGetTipSetByHeight(ctx, latestHeight-filabi.ChainEpoch(lotusLookbackEpochs), latestHead.Key())
	if err != nil {
		return nil, xerrors.Errorf("determining target tipset %d epochs ago failed: %w", lotusLookbackEpochs, err)
	}

	return tipsetAtLookback, nil
}

var collateralCache, _ = lru.New[filabi.ChainEpoch, filbig.Int](128)

func GiBProviderCollateralEstimate(ctx context.Context, sourceEpoch filabi.ChainEpoch) (filbig.Int, error) { //nolint:revive
	if pc, didFind := collateralCache.Get(sourceEpoch); didFind {
		return pc, nil
	}

	ts, err := LotusAPIHeavy.ChainGetTipSetByHeight(ctx, sourceEpoch, lotustypes.EmptyTSK)
	if err != nil {
		return filbig.Zero(), WrErr(err)
	}

	collateralGiB, err := LotusAPIHeavy.StateDealProviderCollateralBounds(ctx, filabi.PaddedPieceSize(1<<30), true, ts.Key())
	if err != nil {
		return filbig.Zero(), WrErr(err)
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
