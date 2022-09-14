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
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	filprovider "github.com/filecoin-project/go-state-types/builtin/v8/miner"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
	lotustypes "github.com/filecoin-project/lotus/chain/types"
	lru "github.com/hashicorp/golang-lru"
	"github.com/labstack/echo/v4"
)

const (
	sigGraceEpochs = 3
	authScheme     = `FIL-SPID-V0`
)

type sigChallenge struct {
	authHdr string
	spID    filaddr.Address
	epoch   int64
	hdr     struct {
		epoch   string
		spid    string
		sigType string
		sigB64  string
	}
}

type verifySigResult struct {
	invalidSigErrstr string
}

var (
	authRe            = regexp.MustCompile(`^` + authScheme + `\s+([0-9]+)\s*;\s*([ft]0[0-9]+)\s*;\s*(2)\s*;\s*([^; ]+)\s*$`)
	challengeCache, _ = lru.New(sigGraceEpochs * 128)
	beaconCache, _    = lru.New(sigGraceEpochs * 4)
)

func spidAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {

		ctx := c.Request().Context()

		var challenge sigChallenge
		challenge.authHdr = c.Request().Header.Get(echo.HeaderAuthorization)
		res := authRe.FindStringSubmatch(challenge.authHdr)
		if len(res) != 5 {
			return retAuthFail(c, "invalid/unexpected FIL-SPID Authorization header '%s'", challenge.authHdr)
		}

		challenge.hdr.epoch, challenge.hdr.spid, challenge.hdr.sigType, challenge.hdr.sigB64 = res[1], res[2], res[3], res[4]

		var err error
		challenge.spID, err = filaddr.NewFromString(challenge.hdr.spid)
		if err != nil {
			return retAuthFail(c, "unexpected FIL-SPID auth address '%s'", challenge.hdr.spid)
		}

		challenge.epoch, err = strconv.ParseInt(challenge.hdr.epoch, 10, 32)
		if err != nil {
			return retAuthFail(c, "unexpected FIL-SPID auth epoch '%s'", challenge.hdr.epoch)
		}

		curFilEpoch := int64(cmn.WallTimeEpoch(time.Now()))
		if curFilEpoch < challenge.epoch {
			return retAuthFail(c, "FIL-SPID auth epoch '%d' is in the future", challenge.epoch)
		}
		if curFilEpoch-challenge.epoch > sigGraceEpochs {
			return retAuthFail(c, "FIL-SPID auth epoch '%d' is too far in the past", challenge.epoch)
		}

		var vsr verifySigResult
		if maybeResult, known := challengeCache.Get(challenge.hdr); known {
			vsr = maybeResult.(verifySigResult)
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
		c.Request().Header.Set("X-EGD-LOGGED-SP", challenge.spID.String())

		// if challenge.spID.String() == "f01" {
		// 	challenge.spID, _ = filaddr.NewFromString("f02")
		// }

		spID := cmn.MustParseActorString(challenge.spID.String())

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
							continent_id,
							sector_log2_size
						]
					FROM egd.providers
					WHERE provider_id = $1
				)
			`,
			spID,
			reqJ,
		).Scan(&requestUUID, &stateEpoch, &spDetails); err != nil {
			return cmn.WrErr(err)
		}

		c.Response().Header().Set("X-EGD-FIL-SPID", challenge.spID.String())

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
			spSectorLog2Size: spDetails[4],
		})

		return next(c)
	}
}

type metaContext struct {
	authedActorID    cmn.ActorID
	stateEpoch       int64
	spSectorLog2Size int16
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

	// a worker can only be a BLS key
	if challenge.hdr.sigType != fmt.Sprintf("%d", filcrypto.SigTypeBLS) {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("unexpected FIL-SPID auth signature type '%s'", challenge.hdr.sigType),
		}, nil
	}

	sig, err := base64.StdEncoding.DecodeString(challenge.hdr.sigB64)
	if err != nil {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("unexpected FIL-SPID auth signature encoding '%s'", challenge.hdr.sigB64),
		}, nil
	}

	var be *lotustypes.BeaconEntry
	if protoBe, didFind := beaconCache.Get(challenge.epoch); didFind {
		be = protoBe.(*lotustypes.BeaconEntry)
	} else {
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
	mi, err := cmn.LotusAPICurState.StateMinerInfo(ctx, challenge.spID, miFinTs.Key())
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
			invalidSigErrstr: fmt.Sprintf("FIL-SPID signature validation failed for auth header '%s'", challenge.authHdr),
		}, nil
	}

	return verifySigResult{}, nil
}
