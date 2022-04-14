package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filaddr "github.com/filecoin-project/go-address"
	"github.com/jackc/pgx/v4"
	"github.com/labstack/echo/v4"
)

func retPayloadAnnotated(c echo.Context, code int, payload types.ResponsePayload, fmsg string, args ...interface{}) error {

	msg := fmt.Sprintf(fmsg, args...)

	var lines []string
	if msg != "" {
		lines = strings.Split(msg, "\n")
		longest := 0
		for _, l := range lines {
			encLen := len(l) + strings.Count(l, `"`)
			if encLen > longest {
				longest = encLen
			}
		}
		for i, l := range lines {
			lines[i] = fmt.Sprintf(" %*s", -longest-1+strings.Count(l, `"`), l)
		}
	}

	r := types.ResponseEnvelope{
		RequestID:    c.Request().Header.Get("X-REQUEST-UUID"),
		ResponseCode: code,
		Response:     payload,
	}

	pv := reflect.ValueOf(payload)
	switch pv.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map:
		l := pv.Len()
		r.ResponseEntries = &l
	}

	if code < 400 {
		r.InfoLines = lines
	} else {
		r.ErrLines = lines

		if reqUUID := c.Request().Header.Get("X-REQUEST-UUID"); reqUUID != "" && msg != "" {

			jPayload, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			if _, err := common.Db.Exec(
				c.Request().Context(),
				`
				UPDATE requests SET
					meta = JSONB_SET(
						JSONB_SET(
							COALESCE( meta, '{}' ),
							'{ error }',
							TO_JSONB( $1::TEXT )
						),
						'{ payload }',
						$2::JSONB
					)
				WHERE
					request_uuid = $3
						AND
					meta->'error' IS NULL
				`,
				msg,
				jPayload,
				reqUUID,
			); err != nil {
				return err
			}
		}
	}

	return c.JSONPretty(code, r, "  ")
}

func retFail(c echo.Context, internalReason interface{}, fMsg string, args ...interface{}) error {

	if reqUUID := c.Request().Header.Get("X-REQUEST-UUID"); reqUUID != "" {

		outJ, err := json.Marshal(struct {
			Err         string      `json:"error"`
			ErrInternal interface{} `json:"internal,omitempty"`
		}{
			Err:         fmt.Sprintf(fMsg, args...),
			ErrInternal: internalReason,
		})
		if err != nil {
			return err
		}

		if _, err = common.Db.Exec(
			c.Request().Context(),
			`UPDATE requests SET meta = $1 WHERE request_uuid = $2`,
			outJ,
			reqUUID,
		); err != nil {
			return err
		}

	}

	return retPayloadAnnotated(
		c,
		http.StatusForbidden, // DO NOT use 400: we rewrite that on the nginx level to normalize a class of transport errors
		nil,
		fMsg, args...,
	)
}

func retAuthFail(c echo.Context, f string, args ...interface{}) error {
	c.Response().Header().Set(echo.HeaderWWWAuthenticate, authScheme)
	return retPayloadAnnotated(
		c,
		http.StatusUnauthorized,
		nil,
		echo.ErrUnauthorized.Error()+"\n\n"+f,
		args...,
	)
}

var providerEligibleCache, _ = ristretto.NewCache(&ristretto.Config{
	NumCounters: 1e7, BufferItems: 64,
	MaxCost: 1024,
	Cost:    func(interface{}) int64 { return 1 },
})

func ineligibleSpMsg(sp filaddr.Address) string {
	return fmt.Sprintf(
		`
At the time of this request Storage provider %s is not eligible to participate in the program
( this state is is almost certainly *temporary* )

Make sure that you:
- Have registered your SP in accordance with the program rules: https://is.gd/filecoin_evergreen_regform
- Are continuing to serve previously onboarded datasets reliably and free of charge
- Have sufficient quality-adjusted power to participate in block rewards
- Have not faulted in the past 48h

If the problem persists, or you believe this is a spurious error: please contact the program
administrators in #slingshot-evergreen over at the Filecoin Slack https://filecoin.io/slack
( direct link: https://filecoinproject.slack.com/archives/C0377FJCG1L )
`,
		sp.String(),
	)
}

func spIneligibleReason(ctx context.Context, sp filaddr.Address) (defIneligibleReason string, defErr error) {

	// do not cache chain-independent factors
	var ignoreChainEligibility bool
	err := common.Db.QueryRow(
		ctx,
		`SELECT COALESCE( (meta->'ignore_chain_eligibility')::BOOL, false )
			FROM providers
		WHERE
			is_active
				AND
			provider_id = $1
		`,
		sp.String(),
	).Scan(&ignoreChainEligibility)
	if err == pgx.ErrNoRows {
		return "provider not marked active", nil
	} else if err != nil {
		return "", err
	} else if ignoreChainEligibility {
		return "", nil
	}

	defer func() {
		if defErr != nil {
			providerEligibleCache.Del(sp.String())
			defIneligibleReason = ""
		} else {
			providerEligibleCache.SetWithTTL(sp.String(), defIneligibleReason, 1, time.Minute)
		}
	}()

	if protoReason, found := providerEligibleCache.Get(sp.String()); found {
		return protoReason.(string), nil
	}

	curTipset, err := common.LotusLookbackTipset(ctx)
	if err != nil {
		return "", err
	}

	mbi, err := common.LotusAPI.MinerGetBaseInfo(ctx, sp, curTipset.Height(), curTipset.Key())
	if err != nil {
		return "", err
	}
	if mbi == nil || !mbi.EligibleForMining {
		return "MBI-ineligible", nil
	}

	// reenable the fault-checks a bit later

	/*

		ydayTipset, err := common.LotusAPI.ChainGetTipSetByHeight(
			ctx,
			curTipset.Height()-filactors.EpochsInDay+1, // X-2880+1
			filtypes.TipSetKey{},
		)
		if err != nil {
			return "", err
		}

		for _, ts := range []*filtypes.TipSet{curTipset, ydayTipset} {
			curMF, err := common.LotusAPI.StateMinerFaults(ctx, sp, ts.Key())
			if err != nil {
				return "", err
			}
			if fc, _ := curMF.Count(); fc != 0 {
				return fmt.Sprintf("%d faults at epoch %d", fc, ts.Height()), nil
			}
		}

	*/

	return "", nil
}
