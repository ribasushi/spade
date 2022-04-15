package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
	filaddr "github.com/filecoin-project/go-address"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	filcrypto "github.com/filecoin-project/go-state-types/crypto"
	filbuild "github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	filtypes "github.com/filecoin-project/lotus/chain/types"
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

		var challenge sigChallenge

		challenge.authHdr = c.Request().Header.Get(echo.HeaderAuthorization)
		res := authRe.FindStringSubmatch(challenge.authHdr)
		if len(res) != 5 {
			return retAuthFail(c, "invalid/unexpected FIL-SPID Authorization header '%s'", challenge.authHdr)
		}

		var err error
		challenge.hdr.epoch, challenge.hdr.spid, challenge.hdr.sigType, challenge.hdr.sigB64 = res[1], res[2], res[3], res[4]

		challenge.spID, err = filaddr.NewFromString(challenge.hdr.spid)
		if err != nil {
			return retAuthFail(c, "unexpected FIL-SPID auth address '%s'", challenge.hdr.spid)
		}

		challenge.epoch, err = strconv.ParseInt(challenge.hdr.epoch, 10, 32)
		if err != nil {
			return retAuthFail(c, "unexpected FIL-SPID auth epoch '%s'", challenge.hdr.epoch)
		}

		curFilEpoch := int64(common.WallTimeEpoch(time.Now()))
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
			vsr, err = verifySig(c, challenge)
			if err != nil {
				return err
			}
			challengeCache.Add(challenge.hdr, vsr)
		}

		if vsr.invalidSigErrstr != "" {
			return retAuthFail(c, vsr.invalidSigErrstr)
		}

		// set only on request object for logging, not part of response
		c.Request().Header.Set("X-LOGGED-SP", challenge.spID.String())

		// if challenge.spID.String() == "f01" {
		// 	challenge.spID, _ = filaddr.NewFromString("f02")
		// }

		req := c.Request()
		reqJ, err := json.Marshal(
			struct {
				Method  string
				URL     *url.URL
				Headers http.Header
			}{
				Method:  req.Method,
				URL:     req.URL,
				Headers: req.Header,
			},
		)
		if err != nil {
			return err
		}

		var requestUUID string
		if err := common.Db.QueryRow(
			c.Request().Context(),
			`
			INSERT INTO requests ( provider_id, request_dump )
				VALUES ( $1, $2 )
			RETURNING request_uuid
			`,
			challenge.spID.String(),
			reqJ,
		).Scan(&requestUUID); err != nil {
			return err
		}

		// set only on request object for logging, not part of response
		c.Request().Header.Set("X-REQUEST-UUID", requestUUID)

		// part of response, also used as globals for rest of request
		c.Response().Header().Set("X-FIL-SPID", challenge.spID.String())
		return next(c)
	}
}

func verifySig(c echo.Context, challenge sigChallenge) (verifySigResult, error) {

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

	ctx := c.Request().Context()

	var be *filtypes.BeaconEntry
	if protoBe, didFind := beaconCache.Get(challenge.epoch); didFind {
		be = protoBe.(*filtypes.BeaconEntry)
	} else {
		be, err = common.LotusAPI.BeaconGetEntry(ctx, filabi.ChainEpoch(challenge.epoch))
		if err != nil {
			return verifySigResult{}, err
		}
		beaconCache.Add(challenge.epoch, be)
	}

	miFinTs, err := common.LotusAPI.ChainGetTipSetByHeight(ctx, filabi.ChainEpoch(challenge.epoch)-filbuild.Finality, types.EmptyTSK)
	if err != nil {
		return verifySigResult{}, err
	}
	mi, err := common.LotusAPI.StateMinerInfo(ctx, challenge.spID, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, err
	}
	workerAddr, err := common.LotusAPI.StateAccountKey(ctx, mi.Worker, miFinTs.Key())
	if err != nil {
		return verifySigResult{}, err
	}

	sigMatch, err := common.LotusAPI.WalletVerify(
		ctx,
		workerAddr,
		append([]byte{0x20, 0x20, 0x20}, be.Data...),
		&crypto.Signature{
			Type: 2,
			Data: []byte(sig),
		},
	)
	if err != nil {
		return verifySigResult{}, err
	}
	if !sigMatch {
		return verifySigResult{
			invalidSigErrstr: fmt.Sprintf("FIL-SPID signature validation failed for auth header '%s'", challenge.authHdr),
		}, nil
	}

	return verifySigResult{}, nil
}
