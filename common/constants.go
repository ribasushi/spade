package cmn

//nolint:revive
const (
	AppName      = "egd"
	PromInstance = "dataprogs_egd"

	FilGenesisUnix      = 1598306400
	FilDefaultLookback  = 10
	ApiMaxTipsetsBehind = 3 // keep in mind that a nul tipset is indistinguishable from loss of sync - do not set too low

	ListEligibleDefaultSize = 500
	ListEligibleMaxSize     = 2 << 20

	ShowRecentFailuresHours = 24

	RequestPieceLockStatement = `SELECT PG_ADVISORY_XACT_LOCK( 1234567890111 )`
)
