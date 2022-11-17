package main

import (
	"fmt"
	"os"
	"strings"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

func main() {

	//
	// Server setup
	e := echo.New()

	// logging middleware must be first
	e.Logger.SetLevel(2) // https://github.com/labstack/gommon/blob/v0.4.0/log/log.go#L40-L42
	e.Use(middleware.LoggerWithConfig(
		middleware.LoggerConfig{
			Skipper:          middleware.DefaultSkipper,
			CustomTimeFormat: "2006-01-02 15:04:05.000",
			Format:           logCfg,
		},
	))

	// routes
	registerRoutes(e)

	//
	// Housekeeping
	e.Any("*", func(c echo.Context) error {
		return retFail(
			c,
			types.ErrInvalidRequest,
			"there is nothing at %s",
			c.Request().RequestURI,
		)
	})
	ctx, cleanup := cmn.TopAppContext(
		func() { e.Close() }, //nolint:errcheck
	)
	defer cleanup()

	//
	// Boot up
	err := (&cli.App{
		Name:   cmn.AppName + "-webapi",
		Before: cmn.CliBeforeSetup,
		Action: func(cctx *cli.Context) error { return e.Start(cctx.String("webapi-listen-address")) },
		Flags: append(
			[]cli.Flag{
				altsrc.NewStringFlag(&cli.StringFlag{
					Name:  "webapi-listen-address",
					Value: "localhost:8080",
				}),
			}, cmn.CliFlags...,
		),
	}).RunContext(ctx, os.Args)
	if err != nil {
		e.Logger.Errorf("%+v", err)
	}
}

var logCfg = fmt.Sprintf("{%s}\n", strings.Join([]string{
	`"time":"${time_custom}"`,
	`"requuid":"${header:X-EGD-REQUEST-UUID}"`,
	`"error":"${error}"`,
	`"status":${status}`,
	`"took":"${latency_human}"`,
	`"sp":"${header:X-EGD-LOGGED-SP}"`,
	`"bytes_in":${bytes_in}`,
	`"bytes_out":${bytes_out}`,
	`"op":"${method} ${host}${uri}"`,
	`"remote_ip":"${remote_ip}"`,
	`"user_agent":"${user_agent}"`,
}, ","))
