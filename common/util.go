package common

import (
	"regexp"

	"github.com/ipfs/go-cid"
)

func CidV1(c cid.Cid) cid.Cid {
	if c.Version() == 1 {
		return c
	}
	return cid.NewCidV1(c.Type(), c.Hash())
}

const (
	cidTrimPrefix = 6
	cidTrimSuffix = 8
)

func TrimCidString(cs string) string {
	if len(cs) <= cidTrimPrefix+cidTrimSuffix+2 {
		return cs
	}
	return cs[0:cidTrimPrefix] + "~" + cs[len(cs)-cidTrimSuffix:]
}

var NonAlphanumRun = regexp.MustCompile(`[^a-zA-Z0-9]+`)
