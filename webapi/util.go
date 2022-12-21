package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	apitypes "github.com/data-preservation-programs/go-spade-apitypes"
	"github.com/dgraph-io/ristretto"
	"github.com/jackc/pgx/v4"
	"github.com/labstack/echo/v4"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/spade/internal/app"
	"golang.org/x/xerrors"
)

func truthyBoolQueryParam(c echo.Context, pname string) bool {
	if !c.QueryParams().Has(pname) {
		return false
	}
	p := strings.ToLower(c.QueryParams().Get(pname))
	if p != "0" && p != "false" && p != "no" {
		return true
	}
	return false
}

func parseUIntQueryParam(c echo.Context, pname string, min, max uint64) (uint64, error) {
	str := c.QueryParam(pname)
	val, err := strconv.ParseUint(str, 10, 64)
	if str == "" || err != nil {
		return 0, xerrors.Errorf("provided '%s' value '%s' is not a valid integer", pname, str)
	}
	if val < min || val > max {
		return 0, xerrors.Errorf("provided '%s' value '%s' is out of bounds ( %d ~ %d )", pname, str, min, max)
	}
	return val, nil
}

func retPayloadAnnotated(c echo.Context, httpCode int, errCode apitypes.APIErrorCode, payload apitypes.ResponsePayload, fmsg string, args ...interface{}) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	msg := fmt.Sprintf(fmsg, args...)

	var lines []string
	if msg != "" {
		lines = strings.Split(msg, "\n")
		longest := 0
		for _, l := range lines {
			encLen := len([]rune(l)) + strings.Count(l, `"`)
			if encLen > longest {
				longest = encLen
			}
		}
		for i, l := range lines {
			lines[i] = fmt.Sprintf(" %*s", -longest-1+strings.Count(l, `"`), l)
		}
	}

	r := apitypes.ResponseEnvelope{
		RequestID:          c.Request().Header.Get("X-SPADE-REQUEST-UUID"),
		ResponseStateEpoch: int64(ctxMeta.stateEpoch),
		ResponseTime:       time.Now(),
		ResponseCode:       httpCode,
		Response:           payload,
	}

	pv := reflect.ValueOf(payload)
	switch pv.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map:
		l := pv.Len()
		r.ResponseEntries = &l
	}

	if httpCode < 400 {
		if errCode != 0 {
			return xerrors.Errorf("HTTP code %d incongruent with internal errCode %d", httpCode, errCode)
		}
		r.InfoLines = lines
	} else {
		r.ErrCode = int(errCode)
		r.ErrSlug = errCode.String()
		r.ErrLines = lines

		if r.RequestID != "" && (msg != "" || errCode != 0) {
			jPayload, err := json.Marshal(payload)
			if err != nil {
				return cmn.WrErr(err)
			}
			if _, err := ctxMeta.Db[app.DbMain].Exec(
				ctx,
				`
				UPDATE spd.requests SET
					request_meta = JSONB_STRIP_NULLS( request_meta || JSONB_BUILD_OBJECT(
						'error', $1::TEXT,
						'error_code', $2::INTEGER,
						'error_slug', $3::TEXT,
						'payload', $4::JSONB
					) )
				WHERE
					request_uuid = $5
				`,
				msg,
				int(errCode),
				errCode.String(),
				jPayload,
				r.RequestID,
			); err != nil {
				return cmn.WrErr(err)
			}
		}
	}

	// FIXME - only prettify on curl and similar
	return c.JSONPretty(httpCode, r, "  ")
}

func curlAuthedForSP(c echo.Context, spID fil.ActorID, path string) string {
	prot := c.Request().Header.Get("X-Forwarded-Proto")
	if prot == "" {
		prot = "http"
	}

	return fmt.Sprintf(
		`echo curl -sLH "Authorization: $( ./fil-spid.bash %s )" %s://%s%s | sh`,
		spID,
		prot,
		c.Request().Host,
		path,
	)
}

func retFail(c echo.Context, errCode apitypes.APIErrorCode, fMsg string, args ...interface{}) error {
	return retPayloadAnnotated(
		c,
		http.StatusForbidden, // DO NOT use 400: we rewrite that on the nginx level to normalize a class of transport errors
		errCode,
		nil,
		fMsg, args...,
	)
}

func retAuthFail(c echo.Context, f string, args ...interface{}) error {
	c.Response().Header().Set(echo.HeaderWWWAuthenticate, authScheme)
	return retPayloadAnnotated(
		c,
		http.StatusUnauthorized,
		apitypes.ErrUnauthorizedAccess,
		nil,
		echo.ErrUnauthorized.Error()+"\n\n"+f,
		args...,
	)
}

// using ristretto here because of SetWithTTL() below
var providerEligibleCache, _ = ristretto.NewCache(&ristretto.Config{
	NumCounters: 1e7, BufferItems: 64,
	MaxCost: 1024,
	Cost:    func(interface{}) int64 { return 1 },
})

func ineligibleSpMsg(spID fil.ActorID) string {
	return fmt.Sprintf(
		`
At the time of this request Storage provider %s is not eligible to use this API
( this state is is almost certainly *temporary* )

Make sure that you:
- Have registered your SP in accordance with each individual tenant
- Are continuing to serve previously onboarded datasets reliably and free of charge
- Have sufficient quality-adjusted power to participate in block rewards
- Have not faulted in the past 48h

If the problem persists, or you believe this is a spurious error: please contact the API
administrators in #spade over at the Fil Slack https://filecoin.io/slack
( direct link: https://filecoinproject.slack.com/archives/C0377FJCG1L )
`,
		spID,
	)
}

func spIneligibleErr(ctx context.Context, spID fil.ActorID) (defIneligibleCode apitypes.APIErrorCode, defErr error) {
	_, _, db, gctx := app.UnpackCtx(ctx)

	// do not cache chain-independent factors
	var ignoreChainEligibility bool
	err := db.QueryRow(
		ctx,
		`
		SELECT COALESCE( ( provider_meta->'ignore_chain_eligibility' )::BOOL, false )
			FROM spd.providers
		WHERE
			NOT COALESCE( ( provider_meta->'globally_inactivated' )::BOOL, false )
				AND
			provider_id = $1
		`,
		spID,
	).Scan(&ignoreChainEligibility)
	if err == pgx.ErrNoRows {
		return apitypes.ErrStorageProviderSuspended, nil
	} else if err != nil {
		return 0, cmn.WrErr(err)
	} else if ignoreChainEligibility {
		return 0, nil
	}

	defer func() {
		if defErr != nil {
			providerEligibleCache.Del(uint64(spID))
			defIneligibleCode = 0
		} else {
			providerEligibleCache.SetWithTTL(uint64(spID), defIneligibleCode, 1, time.Minute)
		}
	}()

	if protoReason, found := providerEligibleCache.Get(uint64(spID)); found {
		return protoReason.(apitypes.APIErrorCode), nil
	}

	curTipset, err := app.DefaultLookbackTipset(ctx)
	if err != nil {
		return 0, cmn.WrErr(err)
	}

	mbi, err := gctx.LotusAPI[app.FilHeavy].MinerGetBaseInfo(ctx, spID.AsFilAddr(), curTipset.Height(), curTipset.Key())
	if err != nil {
		return 0, cmn.WrErr(err)
	}
	if mbi == nil || !mbi.EligibleForMining {
		return apitypes.ErrStorageProviderIneligibleToMine, nil
	}

	return 0, nil
}
