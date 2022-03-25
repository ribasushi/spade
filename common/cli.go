package common

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	lotusapi "github.com/filecoin-project/lotus/api"
	filactors "github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/jackc/pgx/v4/pgxpool"
	lslog "github.com/labstack/gommon/log"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
	"golang.org/x/sys/unix"
)

var IsTerm = isatty.IsTerminal(os.Stderr.Fd())

// singletons populated on start
var (
	LotusAPI            *lotusapi.FullNodeStruct
	Db                  *pgxpool.Pool
	lotusLookbackEpochs uint

	PromURL  string
	PromUser string
	PromPass string
)

var CliFlags = []cli.Flag{
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "lotus-api",
		Value: "http://localhost:1234",
	}),
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:        "lotus-api-token",
		DefaultText: "  {{ private, read from config file }}  ",
	}),
	&cli.UintFlag{
		Name:  "lotus-lookback-epochs",
		Value: FilDefaultLookback,
		DefaultText: fmt.Sprintf("%d epochs / %ds",
			FilDefaultLookback,
			filactors.EpochDurationSeconds*FilDefaultLookback,
		),
		Destination: &lotusLookbackEpochs,
	},
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:  "pg-connstring",
		Value: "postgres:///dbname?user=username&password=&host=/var/run/postgresql",
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

func TopContext(onCleanup func()) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	var o sync.Once
	closer := func() {
		o.Do(func() {
			cancel()
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
		lslog.Warn("termination signal received, cleaning up...")
		closer()
	}()

	return ctx, closer
}

func CliBeforeSetup(cctx *cli.Context) error {
	if err := altsrc.InitInputSourceWithContext(
		CliFlags,
		func(context *cli.Context) (altsrc.InputSourceContext, error) {
			hm, err := os.UserHomeDir()
			if err != nil {
				return nil, err
			}
			return altsrc.NewTomlSourceFromFile(fmt.Sprintf("%s/%s.toml", hm, AppName))
		},
	)(cctx); err != nil {
		return err
	}

	// init the shared DB connection + lotusapi
	// can do it here, since now we know the config
	dbConnCfg, err := pgxpool.ParseConfig(cctx.String("pg-connstring"))
	if err != nil {
		return err
	}
	Db, err = pgxpool.ConnectConfig(cctx.Context, dbConnCfg)
	if err != nil {
		return err
	}

	api := new(lotusapi.FullNodeStruct)
	apiCloser, err := jsonrpc.NewMergeClient(
		cctx.Context,
		cctx.String("lotus-api")+"/rpc/v0",
		"Filecoin",
		[]interface{}{&api.Internal, &api.CommonStruct.Internal},
		http.Header{"Authorization": []string{"Bearer " + cctx.String("lotus-api-token")}},
	)
	if err != nil {
		return err
	}

	go func() {
		<-cctx.Context.Done()
		if Db != nil {
			Db.Close()
		}
		apiCloser()
	}()
	LotusAPI = api

	return nil
}
