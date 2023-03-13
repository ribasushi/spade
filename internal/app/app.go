package app //nolint:revive

import (
	"context"
	"fmt"

	filabi "github.com/filecoin-project/go-state-types/abi"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
)

//nolint:revive
const (
	AppName                       = "spade"
	FilDefaultLookback            = filabi.ChainEpoch(10)
	PolledSPInfoStaleAfterMinutes = 15
)

//nolint:revive
const (
	FilLite = filapitype(iota)
	FilHeavy
)

//nolint:revive
const (
	DbMain = dbtype(iota)
)

type (
	dbtype        int
	DbConns       map[dbtype]*pgxpool.Pool //nolint:revive
	filapitype    int
	FilAPIs       map[filapitype]*fil.LotusAPIClient //nolint:revive
	GlobalContext struct {                           //nolint:revive
		Db       DbConns
		LotusAPI FilAPIs
		Logger   ufcli.Logger
	}
	ctxKey string
)

var ck = ctxKey("♠️")

func GetGlobalCtx(ctx context.Context) GlobalContext { //nolint:revive
	return ctx.Value(ck).(GlobalContext)
}

func UnpackCtx(ctx context.Context) ( //nolint:revive
	origCtx context.Context,
	logger ufcli.Logger,
	mainDB *pgxpool.Pool,
	globalCtx GlobalContext,
) {
	gctx := GetGlobalCtx(ctx)
	return ctx, gctx.Logger, gctx.Db[DbMain], gctx
}

var lotusLookbackEpochs uint

func DefaultLookbackTipset(ctx context.Context) (*fil.LotusTS, error) { //nolint:revive
	return fil.GetTipset(ctx, GetGlobalCtx(ctx).LotusAPI[FilLite], filabi.ChainEpoch(lotusLookbackEpochs))
}

var CommonFlags = []ufcli.Flag{ //nolint:revive
	ufcli.ConfStringFlag(&ufcli.StringFlag{
		Name:  "lotus-api-lite",
		Value: "https://api.chain.love",
	}),
	ufcli.ConfStringFlag(&ufcli.StringFlag{
		Name:  "lotus-api-heavy",
		Value: "http://localhost:1234",
	}),
	ufcli.ConfStringFlag(&ufcli.StringFlag{
		Name:        "lotus-api-heavy-token",
		DefaultText: "  {{ private, read from config file }}  ",
	}),
	&ufcli.UintFlag{
		Name:  "lotus-lookback-epochs",
		Value: uint(FilDefaultLookback),
		DefaultText: fmt.Sprintf("%d epochs / %ds",
			FilDefaultLookback,
			filbuiltin.EpochDurationSeconds*FilDefaultLookback,
		),
		Destination: &lotusLookbackEpochs,
	},
	ufcli.ConfStringFlag(&ufcli.StringFlag{
		Name:  "pg-connstring",
		Value: "postgres:///dbname?user=username&password=&host=/var/run/postgresql",
	}),
	ufcli.ConfStringFlag(&ufcli.StringFlag{
		Name:        "pg-metrics-connstring",
		DefaultText: "defaults to pg-connstring",
	}),
}

func GlobalInit(cctx *ufcli.Context, uf *ufcli.UFcli) (func() error, error) { //nolint:revive

	gctx := GlobalContext{
		Logger:   uf.Logger,
		LotusAPI: make(FilAPIs, 2),
		Db:       make(DbConns, 2),
	}

	apiL, apiLiteCloser, err := fil.LotusAPIClientV0(cctx.Context, cctx.String("lotus-api-lite"), 30, "")
	if err != nil {
		return nil, cmn.WrErr(err)
	}
	gctx.LotusAPI[FilLite] = apiL

	apiH, apiHeavyCloser, err := fil.LotusAPIClientV0(cctx.Context, cctx.String("lotus-api-heavy"), 300, cctx.String("lotus-api-heavy-token"))
	if err != nil {
		return nil, cmn.WrErr(err)
	}
	gctx.LotusAPI[FilHeavy] = apiH

	dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
	if err != nil {
		return nil, cmn.WrErr(err)
	}
	// dbConnCfg.MaxConns = 42
	dbConnCfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		// _, err := c.Exec(ctx, `SET search_path = spade`)
		// _, err := c.Exec(ctx, fmt.Sprintf(`SET STATEMENT_TIMEOUT = %d`, (2*time.Hour).Milliseconds()))
		// return WrErr(err)
		return nil
	}
	gctx.Db[DbMain], err = pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
	if err != nil {
		return nil, cmn.WrErr(err)
	}

	cctx.Context = context.WithValue(cctx.Context, ck, gctx)

	return func() error {
		apiLiteCloser()
		apiHeavyCloser()
		gctx.Db[DbMain].Close()
		return nil
	}, nil
}
