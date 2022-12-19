package main

const (
	listEligibleDefaultSize = 500
	listEligibleMaxSize     = 2 << 20

	showRecentFailuresHours = 24

	requestPieceLockStatement = `SELECT PG_ADVISORY_XACT_LOCK( 1234567890111 )`
)
