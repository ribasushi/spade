package main

import (
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	"github.com/labstack/echo/v4"
)

func apiSpStatus(c echo.Context) error {
	_, ctxMeta := unpackAuthedEchoContext(c)

	return retFail(
		c,
		types.ErrSystemTemporarilyDisabled,
		`
                                            !!! COMING SOON !!!

This area will contain various information regarding the system and the current state of Storage Provider %s
    `,
		ctxMeta.authedActorID.String(),
	)
}
