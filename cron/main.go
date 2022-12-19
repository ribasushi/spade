package main //nolint:revive

import (
	"context"
	"fmt"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"github.com/ribasushi/spade/internal/app"
)

func main() {
	cmdName := app.AppName + "-cron"
	log := logging.Logger(fmt.Sprintf("%s(%d)", cmdName, os.Getpid()))
	logging.SetLogLevel("*", "INFO") //nolint:errcheck

	home, err := os.UserHomeDir()
	if err != nil {
		log.Error(cmn.WrErr(err))
		os.Exit(1)
	}

	(&ufcli.UFcli{
		Logger:   log,
		TOMLPath: fmt.Sprintf("%s/%s.toml", home, app.AppName),
		AppConfig: ufcli.App{
			Name:  cmdName,
			Usage: "Misc background processes for " + app.AppName,
			Commands: []*ufcli.Command{
				pollProviders,
				trackDeals,
				signPending,
				proposePending,
			},
			Flags: app.CommonFlags,
		},
		GlobalInit: app.GlobalInit,
	}).RunAndExit(context.Background())
}
