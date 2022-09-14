package main

import (
	"fmt"
	"net/http"
	"strings"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/webapi/types"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/labstack/echo/v4"
)

func apiSpListEligible(c echo.Context) error {
	ctx, ctxMeta := unpackAuthedEchoContext(c)

	lim := uint64(cmn.ListEligibleDefaultSize)
	if c.QueryParams().Has("limit") {
		var err error
		lim, err = parseUIntQueryParam(c, "limit", 1, cmn.ListEligibleMaxSize)
		if err != nil {
			return retFail(c, types.ErrInvalidRequest, err.Error())
		}
	}

	tenantID := int16(0) // 0 == any
	if c.QueryParams().Has("tenant") {
		tid, err := parseUIntQueryParam(c, "tenant", 1, 1<<15)
		if err != nil {
			return retFail(c, types.ErrInvalidRequest, err.Error())
		}
		tenantID = int16(tid)
	}

	orglocalOnly := truthyBoolQueryParam(c, "orglocal-only")

	// how to list: start small, find setting below
	useQueryFunc := "pieces_eligible_head"

	if c.QueryParams().Has("internal-nolateral") { // secret flag to tune this in flight / figure out optimal values
		if truthyBoolQueryParam(c, "internal-nolateral") {
			useQueryFunc = "pieces_eligible_full"
		}
	} else if lim > cmn.ListEligibleDefaultSize { // deduce from requested lim
		useQueryFunc = "pieces_eligible_full"
	}

	orderedPieces := make([]*struct {
		PieceID       int64
		PieceLog2Size uint8
		pieceSources
		*types.Piece
	}, 0, lim+1)

	if err := pgxscan.Select(
		ctx,
		cmn.Db,
		&orderedPieces,
		fmt.Sprintf("SELECT * FROM egd.%s( $1, $2, $3, $4, $5 )", useQueryFunc),
		ctxMeta.authedActorID,
		lim+1, // ask for one extra, to disambiguate "there is more"
		tenantID,
		truthyBoolQueryParam(c, "include-sourceless"),
		orglocalOnly,
	); err != nil {
		return cmn.WrErr(err)
	}

	info := []string{
		`List of qualifying Piece CIDs together with their availability from various sources.`,
		``,
		`In order to satisfy a FilPlus deal from this deal engine, all you need to do is obtain the `,
		`corresponding .car file (usually by retrieving it from one of the sources within this list).`,
		``,
		`Once you have selected a Piece CID you would like to seal, and are reasonably confident`,
		`you can obtain the data for it - request a deal from the system by invoking the API as`,
		"shown in the corresponding `sample_request_cmd`. You will then receive a deal with 5 minutes,",
		"and can proceed to `lotus-miner storage-deals import-data ...` the corresponding car file.",
		``,
		`In order to see what proposals you have currently pending, you can invoke:`,
		" " + curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/pending_proposals"),
	}

	if orglocalOnly {
		// replace 1st line
		info = append(
			[]string{
				fmt.Sprintf(`List of qualifying Piece CIDs currently active within any provider belonging to the Org of SP %s`, ctxMeta.authedActorID),
				``,
				`This list is ordered by most recently expiring/expired first, and reflects all pieces of data`,
				`that are still present within your own organization. It is recommended you reseal these first,`,
				`as data for them is readily obtainable.`,
				``,
			},
			info[1:]...,
		)
	}

	// we got more than requested - indicate that this set is large
	if uint64(len(orderedPieces)) > lim {
		orderedPieces = orderedPieces[:lim]

		exLim := lim
		if exLim < cmn.ListEligibleDefaultSize {
			exLim = cmn.ListEligibleDefaultSize
		}

		info = append(
			[]string{
				fmt.Sprintf(`NOTE: The complete list of entries has been TRUNCATED to the top %d.`, lim),
				"Use the 'limit' param in your API call to request more of the (possibly very large) list:",
				" " + curlAuthedForSP(c, ctxMeta.authedActorID, fmt.Sprintf("%s?limit=%d", c.Request().URL.Path, (2*exLim)/100*100)),
				"",
			},
			info...,
		)
	}

	srcPtrs := make(piecePointers, len(orderedPieces))
	ret := make(types.ResponsePiecesEligible, len(orderedPieces))
	for i, p := range orderedPieces {
		p.PaddedPieceSize = 1 << p.PieceLog2Size
		p.SampleRequestCmd = curlAuthedForSP(c, ctxMeta.authedActorID, "/sp/request_piece/"+p.PieceCid)
		ret[i] = p.Piece

		p.pieceSources.sourcesPointer = &ret[i].Sources
		p.pieceSources.pieceCid = p.PieceCid
		srcPtrs[p.PieceID] = p.pieceSources
	}

	if err := injectSources(ctx, srcPtrs); err != nil {
		return cmn.WrErr(err)
	}

	return retPayloadAnnotated(c, http.StatusOK, 0, ret, strings.Join(info, "\n"))
}
