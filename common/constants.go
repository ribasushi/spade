package common

import (
	filactor "github.com/filecoin-project/specs-actors/actors/builtin"
)

//nolint:revive
const (
	AppName      = "evergreen-dealer"
	PromInstance = "dataprogs_evergreen"

	FilGenesisUnix      = 1598306400
	FilDefaultLookback  = 10
	ApiMaxTipsetsBehind = 3 // keep in mind that a nul tipset is indistinguishable from loss of sync - do not set too low

	MaxOutstandingGiB              = int64(4 * 1024)
	ProposalStartDelayFromMidnight = (72 + 16) * filactor.EpochsInHour
	ProposalDuration               = 532 * filactor.EpochsInDay
)
