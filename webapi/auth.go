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

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filprovider "github.com/filecoin-project/go-state-types/builtin/v9/miner"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
)

const (
	sigGraceEpochs = 3
	authScheme     = `FIL-SPID-V0`
)

type rawHdr struct {
	epoch  string
	addr   string
	sigB64 string
}
type sigChallenge struct {
	authHdr string
	addr    filaddr.Address
	epoch   int64
	hdr     rawHdr
}

type verifySigResult struct {
	invalidSigErrstr string
}

var (
	authRe            = regexp.MustCompile(`^` + authScheme + `\s+([0-9]+)\s*;\s*([ft]0[0-9]+)\s*;(?:\s*(2)\s*;)?\s*([^; ]+)\s*$`)
	challengeCache, _ = lru.New[rawHdr, verifySigResult](sigGraceEpochs * 128)
	beaconCache, _    = lru.New[int64, *lotustypes.BeaconEntry](sigGraceEpochs * 4)
)

func spidAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()

		var challenge sigChallenge
		challenge.authHdr = c.Request().Header.Get(echo.HeaderAuthorization)
		res := authRe.FindStringSubmatch(challenge.authHdr)

		// 5 because I was dumb and included the sigtype as part of the challenge
		if len(res) == 5 {
			challenge.hdr.epoch, challenge.hdr.addr, _, challenge.hdr.sigB64 = res[1], res[2], res[3], res[4]
		} else if len(res) == 4 {
			challenge.hdr.epoch, challenge.hdr.addr, challenge.hdr.sigB64 = res[1], res[2], res[3]
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

		curFilEpoch := int64(cmn.WallTimeEpoch(time.Now()))
		if curFilEpoch < challenge.epoch {
			return retAuthFail(c, "%s auth epoch '%d' is in the future", authScheme, challenge.epoch)
		}
		if curFilEpoch-challenge.epoch > sigGraceEpochs {
			return retAuthFail(c, "%s auth epoch '%d' is too far in the past", authScheme, challenge.epoch)
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
		c.Request().Header.Set("X-EGD-LOGGED-SP", challenge.addr.String())

		// if challenge.addr.String() == "f01" {
		// 	challenge.addr, _ = filaddr.NewFromString("f02")
		// }

		spID := cmn.MustParseActorString(challenge.addr.String())

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
		var spInfo types.SPInfo
		var spInfoLastPoll *time.Time
		if err := cmn.Db.QueryRow(
			ctx,
			`
			INSERT INTO egd.requests ( provider_id, request_dump )
				VALUES ( $1, $2 )
			RETURNING
				request_uuid,
				( SELECT ( metadata->'market_state'->'epoch' )::INTEGER FROM egd.global ),
				(
					SELECT
						ARRAY[
							org_id,
							city_id,
							country_id,
							continent_id
						]
					FROM egd.providers
					WHERE provider_id = $1
				),
				(
					SELECT info
						FROM egd.providers_info
					WHERE provider_id = $1
				),
				(
					SELECT provider_last_polled
						FROM egd.providers_info
					WHERE provider_id = $1
				)
			`,
			spID,
			reqJ,
		).Scan(&requestUUID, &stateEpoch, &spDetails, &spInfo, &spInfoLastPoll); err != nil {
			return cmn.WrErr(err)
		}

		c.Response().Header().Set("X-EGD-FIL-SPID", challenge.addr.String())

		// set on both request (for logging ) and response object
		c.Request().Header.Set("X-EGD-REQUEST-UUID", requestUUID)
		c.Response().Header().Set("X-EGD-REQUEST-UUID", requestUUID)

		c.Set("egd-meta", metaContext{
			stateEpoch:       stateEpoch,
			authedActorID:    spID,
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
	authedActorID    cmn.ActorID
	stateEpoch       int64
	spInfo           types.SPInfo
	spInfoLastPolled *time.Time
	spOrgID          int16
	spCityID         int16
	spCountryID      int16
	spContinentID    int16
}

func unpackAuthedEchoContext(c echo.Context) (context.Context, metaContext) {
	meta, _ := c.Get("egd-meta").(metaContext) // ignore potential nil error on purpose
	return c.Request().Context(), meta
}

func verifySig(ctx context.Context, challenge sigChallenge) (verifySigResult, error) {

	sig, err := base64.StdEncoding.DecodeString(challenge.hdr.sigB64)
	if err != nil {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("unexpected %s auth signature encoding '%s'", authScheme, challenge.hdr.sigB64),
		}, nil
	}

	be, didFind := beaconCache.Get(challenge.epoch)
	if !didFind {
		be, err = cmn.LotusAPIHeavy.StateGetBeaconEntry(ctx, filabi.ChainEpoch(challenge.epoch))
		if err != nil {
			return verifySigResult{}, cmn.WrErr(err)
		}
		beaconCache.Add(challenge.epoch, be)
	}

	miFinTs, err := cmn.LotusAPICurState.ChainGetTipSetByHeight(ctx, filabi.ChainEpoch(challenge.epoch)-filprovider.ChainFinality, lotustypes.EmptyTSK)
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}
	mi, err := cmn.LotusAPICurState.StateMinerInfo(ctx, challenge.addr, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}
	workerAddr, err := cmn.LotusAPICurState.StateAccountKey(ctx, mi.Worker, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, cmn.WrErr(err)
	}

	sigMatch, err := cmn.LotusAPIHeavy.WalletVerify(
		ctx,
		workerAddr,
		append([]byte{0x20, 0x20, 0x20}, be.Data...),
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
