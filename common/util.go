package common

import (
	"regexp"
)

const (
	cidTrimPrefix = 6
	cidTrimSuffix = 8
)

func TrimCidString(cs string) string { //nolint:revive
	if len(cs) <= cidTrimPrefix+cidTrimSuffix+2 {
		return cs
	}
	return cs[0:cidTrimPrefix] + "~" + cs[len(cs)-cidTrimSuffix:]
}

var NonAlphanumRun = regexp.MustCompile(`[^a-zA-Z0-9]+`) //nolint:revive
