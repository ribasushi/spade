package main //nolint:revive

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	fslock "github.com/ipfs/go-fs-lock"
	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus"
	prometheuspush "github.com/prometheus/client_golang/prometheus/push"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger(fmt.Sprintf("%s-cron(%d)", cmn.AppName, os.Getpid()))

func main() {
	ctx, cleanup := cmn.TopAppContext(nil)
	defer cleanup()

	// wrap in a defer to always capture endstate/send a metric, even under panic()s
	var (
		t0             time.Time
		err            error
		currentCmd     string
		currentCmdLock io.Closer
	)
	defer func() {

		// shared log/metric emitter
		// ( lock-contention does not count, see invocation below )
		emitEndLogs := func(logSuccess bool) {

			took := time.Since(t0).Truncate(time.Millisecond)
			cmdFqName := cmn.NonAlphanumRun.ReplaceAllString(cmn.AppName+"_cron_"+currentCmd, `_`)
			logHdr := fmt.Sprintf("=== FINISH '%s' run", currentCmd)
			logArgs := []interface{}{
				"success", logSuccess,
				"took", took.String(),
			}

			tookGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: fmt.Sprintf("%s_run_time", cmdFqName),
				Help: "How long did the job take (in milliseconds)",
			})
			tookGauge.Set(float64(took.Milliseconds()))
			successGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: fmt.Sprintf("%s_success", cmdFqName),
				Help: "Whether the job completed with success(1) or failure(0)",
			})

			if logSuccess {
				log.Infow(logHdr, logArgs...)
				successGauge.Set(1)
			} else {
				log.Warnw(logHdr, logArgs...)
				successGauge.Set(0)
			}

			if cmn.PromURL != "" {
				if promErr := prometheuspush.New(cmn.PromURL, cmn.NonAlphanumRun.ReplaceAllString(currentCmd, "_")).
					Grouping("instance", cmn.NonAlphanumRun.ReplaceAllString(cmn.PromInstance, "_")).
					BasicAuth(cmn.PromUser, cmn.PromPass).
					Collector(tookGauge).
					Collector(successGauge).
					Push(); promErr != nil {
					log.Warnf("push of prometheus metrics to '%s' failed: %s", cmn.PromURL, promErr)
				}
			}
		}

		// a panic condition takes precedence
		if r := recover(); r != nil {
			if err == nil {
				err = xerrors.Errorf("panic encountered: %w", r)
			} else {
				err = xerrors.Errorf("panic encountered (in addition to error '%s'): %w", err, r)
			}
		}

		if err != nil {
			// if we are not interactive - be quiet on a failed lock
			if !cmn.IsTerm && errors.As(err, new(fslock.LockedError)) {
				cleanup()
				os.Exit(1)
			}

			log.Errorf("%+v", err)
			if currentCmdLock != nil {
				emitEndLogs(false)
			}
			cleanup()
			os.Exit(1)
		} else if currentCmdLock != nil {
			emitEndLogs(true)
		}
	}()

	t0 = time.Now()
	// the function ends after this block, err is examined in the defer above
	// organized in this bizarre way in order to catch panics
	err = (&cli.App{
		Name:  cmn.AppName + "-cron",
		Usage: "Misc background processes for " + cmn.AppName,
		Commands: []*cli.Command{
			// updateProviders,
			trackDeals,
			pushMetrics,
			signPending,
			proposePending,
		},
		Flags: cmn.CliFlags,
		// obtains locks and emits the proper init loglines
		Before: func(cctx *cli.Context) error {
			if err := cmn.CliBeforeSetup(cctx); err != nil {
				return cmn.WrErr(err)
			}

			// figure out what is the command that was invoked
			if len(os.Args) > 1 {

				cmdNames := make(map[string]string)
				for _, c := range cctx.App.Commands {
					cmdNames[c.Name] = c.Name
					for _, a := range c.Aliases {
						cmdNames[a] = c.Name
					}
				}

				var firstCmdOccurrence string
				for i := 1; i < len(os.Args); i++ {

					// if we are in help context - no locks and no start/stop timers
					if os.Args[i] == `-h` || os.Args[i] == `--help` {
						return nil
					}

					if firstCmdOccurrence != "" {
						continue
					}
					firstCmdOccurrence = cmdNames[os.Args[i]]
				}

				// help, wrong cmd or something
				if firstCmdOccurrence == "" || firstCmdOccurrence == "help" {
					return nil
				}

				currentCmd = firstCmdOccurrence

				var err error
				if currentCmdLock, err = fslock.Lock(os.TempDir(), "egd-cron-"+currentCmd); err != nil {
					return cmn.WrErr(err)
				}
				log.Infow(fmt.Sprintf("=== BEGIN '%s' run", currentCmd))
			}

			return nil
		},
	}).RunContext(ctx, os.Args)
}
