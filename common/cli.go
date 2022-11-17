package cmn //nolint:revive

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	filbuiltin "github.com/filecoin-project/go-state-types/builtin"
	lotusapi "github.com/filecoin-project/lotus/api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
	"golang.org/x/sys/unix"
)

var (
	IsTerm = isatty.IsTerminal(os.Stderr.Fd()) //nolint:revive
	log    = logging.Logger(fmt.Sprintf("%s(%d)", AppName, os.Getpid()))
)

// singletons populated on start
var (
	Db                  *pgxpool.Pool
	LotusAPICurState    *lotusapi.FullNodeStruct
	LotusAPIHeavy       *lotusapi.FullNodeStruct
	lotusLookbackEpochs uint

	PromURL  string
	PromUser string
	PromPass string
)

var CliFlags = []cli.Flag{ //nolint:revive
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "lotus-api-curstate",
		Value: "https://api.chain.love",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "lotus-api-heavy",
		Value: "http://localhost:1234",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "lotus-api-heavy-token",
		DefaultText: "  {{ private, read from config file }}  ",
	}),
	&cli.UintFlag{
		Name:  "lotus-lookback-epochs",
		Value: FilDefaultLookback,
		DefaultText: fmt.Sprintf("%d epochs / %ds",
			FilDefaultLookback,
			filbuiltin.EpochDurationSeconds*FilDefaultLookback,
		),
		Destination: &lotusLookbackEpochs,
	},
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "pg-connstring",
		Value: "postgres:///dbname?user=username&password=&host=/var/run/postgresql",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "pg-metrics-connstring",
		DefaultText: "defaults to pg-connstring",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "prometheus_push_url",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
		Destination: &PromURL,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "prometheus_push_user",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
		Destination: &PromUser,
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "prometheus_push_pass",
		DefaultText: "  {{ private, read from config file }}  ",
		Hidden:      true,
		Destination: &PromPass,
	}),
}

func TopAppContext(onCleanup func()) (context.Context, func()) { //nolint:revive

	logging.SetLogLevel("*", "INFO") //nolint:errcheck

	// when using lp2p this will fire arbitrarily driven by rand()
	// there is no value doing so in a CLI setting
	// https://github.com/libp2p/go-libp2p/blob/master/core/canonicallog/canonicallog.go
	logging.SetLogLevel("canonical-log", "ERROR") //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())

	var o sync.Once
	closer := func() {
		o.Do(func() {
			cancel()
			if Db != nil {
				Db.Close()
			}
			if onCleanup != nil {
				onCleanup()
			}
			time.Sleep(250 * time.Millisecond) // give a bit of time for various parts to close
		})
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
		<-sigs
		log.Warn("termination signal received, cleaning up...")
		closer()
	}()

	return ctx, closer
}

func CliBeforeSetup(cctx *cli.Context) error { //nolint:revive
	if err := altsrc.InitInputSourceWithContext(
		CliFlags,
		func(context *cli.Context) (altsrc.InputSourceContext, error) {
			hm, err := os.UserHomeDir()
			if err != nil {
				return nil, WrErr(err)

			}
			return altsrc.NewTomlSourceFromFile(fmt.Sprintf("%s/%s.toml", hm, AppName))
		},
	)(cctx); err != nil {
		return WrErr(err)
	}

	// init the shared DB connection + lotusapi
	// can do it here, since now we know the config
	dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
	if err != nil {
		return WrErr(err)
	}
	dbConnCfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		// _, err := c.Exec(ctx, `SET search_path = egd`)
		// return WrErr(err)
		return nil
	}
	Db, err = pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
	if err != nil {
		return WrErr(err)
	}

	apiCur := new(lotusapi.FullNodeStruct)
	apiCurCloser, err := jsonrpc.NewMergeClient(
		cctx.Context,
		cctx.String("lotus-api-curstate")+"/rpc/v0",
		"Filecoin",
		[]interface{}{&apiCur.Internal, &apiCur.CommonStruct.Internal},
		http.Header{},
	)
	if err != nil {
		return WrErr(err)
	}

	apiHeavy := new(lotusapi.FullNodeStruct)
	apiHeavyCloser, err := jsonrpc.NewMergeClient(
		cctx.Context,
		cctx.String("lotus-api-heavy")+"/rpc/v0",
		"Filecoin",
		[]interface{}{&apiHeavy.Internal, &apiHeavy.CommonStruct.Internal},
		http.Header{"Authorization": []string{"Bearer " + cctx.String("lotus-api-heavy-token")}},
		jsonrpc.WithTimeout(300*time.Second),
	)
	if err != nil {
		return WrErr(err)
	}

	LotusAPICurState = apiCur
	LotusAPIHeavy = apiHeavy

	go func() {
		<-cctx.Context.Done()
		if Db != nil {
			Db.Close()
		}
		apiCurCloser()
		apiHeavyCloser()
	}()

	return nil
}
