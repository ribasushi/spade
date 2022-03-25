package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/evergreen-dealer/common"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

func main() {

	//
	// Server setup
	e := echo.New()

	// logging middleware must be first
	e.Logger.SetLevel(log.INFO)
	e.Use(middleware.LoggerWithConfig(
		middleware.LoggerConfig{
			Skipper:          middleware.DefaultSkipper,
			CustomTimeFormat: "2006-01-02 15:04:05.000",
			Format:           logCfg,
		},
	))

	// this is our auth-provider
	e.Use(spidAuth)

	// routes
	e.GET("eligible_pieces/sp_local", apiListEligible)
	e.GET("eligible_pieces/anywhere", apiListEligible)
	e.GET("request_piece/:pieceCID", apiRequestPiece)
	e.GET("pending_proposals", apiListPendingProposals)
	e.Any("*", func(c echo.Context) error {
		return retFail(
			c,
			nil,
			"there is nothing useful at %s",
			c.Request().RequestURI,
		)
	})

	//
	// Housekeeping
	ctx, cleanup := common.TopContext(
		func() { e.Close() }, //nolint:errcheck
	)
	defer cleanup()

	//
	// Boot up
	err := (&cli.App{
		Name:   common.AppName + "-webapi",
		Before: common.CliBeforeSetup,
		Action: func(cctx *cli.Context) error { return e.Start(cctx.String("webapi-listen-address")) },
		Flags: append(
			[]cli.Flag{
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:  "webapi-listen-address",
					Value: "localhost:8080",
				}),
			}, common.CliFlags...,
		),
	}).RunContext(ctx, os.Args)
	if err != nil {
		log.Error(err)
	}
}

var logCfg = fmt.Sprintf("{%s}\n", strings.Join([]string{
	`"time":"${time_custom}"`,
	`"requuid":"${header:X-REQUEST-UUID}"`,
	`"sp":"${header:X-AUTHED-SP}"`,
	`"remote_ip":"${remote_ip}"`,
	`"user_agent":"${user_agent}"`,
	`"bytes_in":${bytes_in}`,
	`"bytes_out":${bytes_out}`,
	`"op":"${method} ${host}${uri}"`,
	`"status":${status}`,
	`"error":"${error}"`,
	`"latency_human":"${latency_human}"`,
}, ","))
