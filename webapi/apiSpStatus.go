package main

import (
	apitypes "github.com/data-preservation-programs/go-spade-apitypes"
	"github.com/labstack/echo/v4"
)

func apiSpStatus(c echo.Context) error {
	_, ctxMeta := unpackAuthedEchoContext(c)

	return retFail(
		c,
		apitypes.ErrSystemTemporarilyDisabled,
		`
                                            !!! COMING SOON !!!

This area will contain various information regarding the system and the current state of Storage Provider %s
    `,
		ctxMeta.authedActorID.String(),
	)
}
