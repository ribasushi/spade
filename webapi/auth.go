package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	apitypes "github.com/data-preservation-programs/go-spade-apitypes"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filprovider "github.com/filecoin-project/go-state-types/builtin/v9/miner"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/spade/internal/app"
)

const (
	sigGraceEpochs = 3
	authScheme     = `FIL-SPID-V0`
)

type rawHdr struct {
	epoch  string
	addr   string
	sigB64 string
	arg    string
}
type sigChallenge struct {
	authHdr string
	addr    filaddr.Address
	epoch   int64
	arg     []byte
	hdr     rawHdr
}

type verifySigResult struct {
	invalidSigErrstr string
}

var (
	spAuthRe = regexp.MustCompile(
		`^` + authScheme + `\s+` +
			// fil epoch
			`([0-9]+)` + `\s*;\s*` +
			// spID
			`([ft]0[0-9]+)` + `\s*;\s*` +
			// legacy crap, remove once contract signing in place for everyone
			`(?:\s*2\s*;)?` +
			// signature
			`([^; ]+)` +
			// optional signed argument
			`(?:\s*\;\s*([^; ]+))?` +
			`\s*$`,
	)
	challengeCache, _ = lru.New[rawHdr, verifySigResult](sigGraceEpochs * 128)
	beaconCache, _    = lru.New[int64, *lotustypes.BeaconEntry](sigGraceEpochs * 4)
)

func spidAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()

		var challenge sigChallenge
		challenge.authHdr = c.Request().Header.Get(echo.HeaderAuthorization)
		res := spAuthRe.FindStringSubmatch(challenge.authHdr)

		if len(res) == 5 {
			challenge.hdr.epoch, challenge.hdr.addr, challenge.hdr.sigB64, challenge.hdr.arg = res[1], res[2], res[3], res[4]
		} else {
			return retAuthFail(c, "invalid/unexpected %s Authorization header '%s'", authScheme, challenge.authHdr)
		}

		var err error
		challenge.addr, err = filaddr.NewFromString(challenge.hdr.addr)
		if err != nil {
			return retAuthFail(c, "unexpected %s auth address '%s'", authScheme, challenge.hdr.addr)
		}

		challenge.epoch, err = strconv.ParseInt(challenge.hdr.epoch, 10, 32)
		if err != nil {
			return retAuthFail(c, "unexpected %s auth epoch '%s'", authScheme, challenge.hdr.epoch)
		}

		curFilEpoch := int64(fil.WallTimeEpoch(time.Now()))
		if curFilEpoch < challenge.epoch {
			return retAuthFail(c, "%s auth epoch '%d' is in the future", authScheme, challenge.epoch)
		}
		if curFilEpoch-challenge.epoch > sigGraceEpochs {
			return retAuthFail(c, "%s auth epoch '%d' is too far in the past", authScheme, challenge.epoch)
		}

		challenge.arg, err = base64.StdEncoding.DecodeString(challenge.hdr.arg)
		if err != nil {
			return retAuthFail(c, "unable to decode optional argument: %s", err.Error())
		}

		var vsr verifySigResult
		if maybeResult, known := challengeCache.Get(challenge.hdr); known {
			vsr = maybeResult
		} else {
			vsr, err = verifySig(ctx, challenge)
			if err != nil {
				return cmn.WrErr(err)
			}
			challengeCache.Add(challenge.hdr, vsr)
		}

		if vsr.invalidSigErrstr != "" {
			return retAuthFail(c, vsr.invalidSigErrstr)
		}

		// set only on request object for logging, not part of response
		c.Request().Header.Set("X-SPADE-LOGGED-SP", challenge.addr.String())

		// if challenge.addr.String() == "f01" {
		// 	challenge.addr, _ = filaddr.NewFromString("f02")
		// }

		spID := fil.MustParseActorString(challenge.addr.String())

		reqCopy := c.Request().Clone(ctx)
		// do not need to store any IPs anywhere in the DB
		for _, strip := range []string{
			"X-Real-Ip", "X-Forwarded-For", "Cf-Connecting-Ip",
		} {
			delete(reqCopy.Header, strip)
		}
		reqJ, err := json.Marshal(
			struct {
				Method  string
				Host    string
				URL     *url.URL
				Headers http.Header
			}{
				Method:  reqCopy.Method,
				Host:    reqCopy.Host,
				URL:     reqCopy.URL,
				Headers: reqCopy.Header,
			},
		)
		if err != nil {
			return cmn.WrErr(err)
		}

		var requestUUID string
		var stateEpoch int64
		var spDetails []int16
		var spInfo apitypes.SPInfo
		var spInfoLastPoll *time.Time
		if err := app.GetGlobalCtx(ctx).Db[app.DbMain].QueryRow(
			ctx,
			`
			INSERT INTO spd.requests ( provider_id, request_dump )
				VALUES ( $1, $2 )
			RETURNING
				request_uuid,
				( SELECT ( metadata->'market_state'->'epoch' )::INTEGER FROM spd.global ),
				(
					SELECT
						ARRAY[
							org_id,
							city_id,
							country_id,
							continent_id
						]
					FROM spd.providers
					WHERE provider_id = $1
				),
				(
					SELECT info
						FROM spd.providers_info
					WHERE provider_id = $1
				),
				(
					SELECT provider_last_polled
						FROM spd.providers_info
					WHERE provider_id = $1
				)
			`,
			spID,
			reqJ,
		).Scan(&requestUUID, &stateEpoch, &spDetails, &spInfo, &spInfoLastPoll); err != nil {
			return cmn.WrErr(err)
		}

		c.Response().Header().Set("X-SPADE-FIL-SPID", challenge.addr.String())

		// set on both request (for logging ) and response object
		c.Request().Header.Set("X-SPADE-REQUEST-UUID", requestUUID)
		c.Response().Header().Set("X-SPADE-REQUEST-UUID", requestUUID)

		c.Set("♠️", metaContext{
			GlobalContext:    app.GetGlobalCtx(ctx),
			stateEpoch:       stateEpoch,
			authedActorID:    spID,
			authArg:          challenge.arg,
			spOrgID:          spDetails[0],
			spCityID:         spDetails[1],
			spCountryID:      spDetails[2],
			spContinentID:    spDetails[3],
			spInfo:           spInfo,
			spInfoLastPolled: spInfoLastPoll,
		})

		return next(c)
	}
}

type metaContext struct {
	app.GlobalContext
	authedActorID    fil.ActorID
	stateEpoch       int64
	spInfo           apitypes.SPInfo
	spInfoLastPolled *time.Time
	spOrgID          int16
	spCityID         int16
	spCountryID      int16
	spContinentID    int16
	authArg          []byte
}

func unpackAuthedEchoContext(c echo.Context) (context.Context, metaContext) {
	meta, _ := c.Get("♠️").(metaContext) // ignore potential nil error on purpose
	return c.Request().Context(), meta
}

func verifySig(ctx context.Context, challenge sigChallenge) (verifySigResult, error) {

	sig, err := base64.StdEncoding.DecodeString(challenge.hdr.sigB64)
	if err != nil {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("unexpected %s auth signature encoding '%s'", authScheme, challenge.hdr.sigB64),
		}, nil
	}

	apis := app.GetGlobalCtx(ctx).LotusAPI
	hAPI := apis[app.FilHeavy]
	lAPI := apis[app.FilLite]

	be, didFind := beaconCache.Get(challenge.epoch)
	if !didFind {
		be, err = hAPI.StateGetBeaconEntry(ctx, filabi.ChainEpoch(challenge.epoch))
		if err != nil {
			return verifySigResult{}, cmn.WrErr(err)
		}
		beaconCache.Add(challenge.epoch, be)
	}

	miFinTs, err := lAPI.ChainGetTipSetByHeight(ctx, filabi.ChainEpoch(challenge.epoch)-filprovider.ChainFinality, lotustypes.EmptyTSK)
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}
	mi, err := lAPI.StateMinerInfo(ctx, challenge.addr, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}
	workerAddr, err := lAPI.StateAccountKey(ctx, mi.Worker, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}

	sigMatch, err := hAPI.WalletVerify(
		ctx,
		workerAddr,
		append(append([]byte{0x20, 0x20, 0x20}, be.Data...), challenge.arg...),
		&filcrypto.Signature{
			Type: filcrypto.SigTypeBLS, // worker keys are always BLS
			Data: []byte(sig),
		},
	)
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}

	if !sigMatch {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("%s signature validation failed for auth header '%s'", authScheme, challenge.authHdr),
		}, nil
	}
	return verifySigResult{}, nil
}
