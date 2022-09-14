package main

import "github.com/labstack/echo/v4"

//
// This lists in one place all recognized routes & parameters
// FIXME - we should make an openapi or something for this...
//
func registerRoutes(e *echo.Echo) {
	spRoutes := e.Group("/sp", spidAuth)

	//
	// /status produces human and machine readable information about the system and the currently-authenticated SP
	//
	// Recognized parameters: none
	//
	spRoutes.GET("/status", apiSpStatus)

	//
	// /eligible_pieces produces a listing of PieceCIDs that a storage provider is eligible to receive a deal for.
	// The list is dynamic and offers a near-real-time view specific to the authenticated SP answering:
	// "What can I reserve/request right this moment"
	//
	// Recognized parameters:
	//
	// - limit = <integer>
	//   How many results to return at most
	//   default=cmn.ListEligibleDefaultSize
	//
	// - tenant = <integer>
	//   Restrict the list to only pieces claimed by this numeric TenantID. No restriction if unspecified.
	//
	// - include-sourceless = <boolean>
	//   When true the result includes eligible pieces without any known sources. Such pieces are omitted by default.
	//
	// - orglocal-only = <boolean>
	//   When true restrict result only to pieces with active filecoin sources within your own Org.
	//
	spRoutes.GET("/eligible_pieces", apiSpListEligible)

	//
	// /pending_proposals produces a list of current outstanding reservations, recent errors and various statistics.
	//
	// Recognized parameters: none
	//
	spRoutes.GET("/pending_proposals", apiSpListPendingProposals)

	//
	// The following are actually logical POSTs, keep as GET for simplicity/redirectability
	// ( plus we do have a rather tight auth-header timing + proper locking and all )
	//

	//
	// /request_piece/:pieceCID is used to request a deal proposal (and thus reservation) for a specific
	// PieceCID. The call will fail with HTTP 403 + a corresponding internal error code if the SP
	// is not eligible to receive a deal for this PieceCID. On success a deal proposal is queued and
	// delivered to the SP by a periodic task, executed outside of this webapp.
	//
	// Recognized parameters:
	//
	// - tenant = <integer>
	//   Restrict the deal proposal to a specific TenantID. The call will fail if the deal can not be granted by
	//   the specified tenant even if it would be allowed by a different tenant with interest in the same piece.
	//
	spRoutes.GET("/request_piece/:pieceCID", apiSpRequestPiece)
}
