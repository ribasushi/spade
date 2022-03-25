package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/evergreen-dealer/common"
	filaddr "github.com/filecoin-project/go-address"
	"github.com/jackc/pgx/v4"
	"github.com/labstack/echo/v4"
	"golang.org/x/xerrors"
)

type dataSource interface {
	srcType() string
	expiryUnixNano() int64
	expiryCoarse() int64
	sysID() string
}

type filSource struct {
	SourceType string `json:"source_type"`
	ProviderID string `json:"provider_id"`

	// filecoin specific
	DealID            int64      `json:"deal_id"`
	DealExpiration    time.Time  `json:"deal_expiration"`
	IsFilplus         bool       `json:"is_filplus"`
	SectorID          *string    `json:"sector_id"`
	SectorExpires     *time.Time `json:"sector_expires"`
	SampleRetrieveCmd string     `json:"sample_retrieve_cmd"`

	expUnixNano int64
	expCoarse   int64
	sysIDStr    string
}

var _ dataSource = &filSource{}

type piece struct {
	PieceCid         string       `json:"piece_cid"`
	Dataset          *string      `json:"dataset"`
	PaddedPieceSize  uint64       `json:"padded_piece_size"`
	PayloadCids      []string     `json:"payload_cids"`
	Sources          []dataSource `json:"sources"`
	SampleRequestCmd string       `json:"sample_request_cmd"`
}

func apiListEligible(c echo.Context) error {
	ctx := c.Request().Context()

	// log.Info("entered")

	sp, err := filaddr.NewFromString(c.Response().Header().Get("X-FIL-SPID"))
	if err != nil {
		return err
	}

	lim := uint64(128)
	limStr := c.QueryParams().Get("limit")
	if limStr != "" {
		lim, err = strconv.ParseUint(limStr, 10, 64)
		if err != nil {
			return retFail(c, nil, "provided limit '%s' is not a valid integer", limStr)
		}
	}

	internalReason, err := spIneligibleReason(ctx, sp)
	if err != nil {
		return err
	} else if internalReason != "" {
		return retFail(c, internalReason, ineligibleSpMsg(sp))
	}

	// only query them in the `anywhere` case
	var spOrgID, spCity, spCountry, spContinent string
	var maxPerOrg, maxPerCity, maxPerCountry, maxPerContinent, programMax int64

	commonInfoFooter := strings.Join([]string{
		`Once you have selected a Piece CID you would like to renew, and are reasonably confident`,
		`you can obtain the data for it - request a deal from the system by invoking the API as`,
		"shown in the corresponding `sample_request_cmd`. You will then receive a deal with 10 minutes,",
		"and can proceed to `lotus-miner storage-deals import-data ...` the corresponding car file.",
		``,
		`In order to see what proposals you have currently pending, you can invoke:`,
		fmt.Sprintf(` echo curl -sLH "Authorization: $( ./fil-spid.bash %s )" 'https://api.evergreen.filecoin.io/pending_proposals' | sh `, sp.String()),
	}, "\n")

	// log.Info("authed")

	var rows pgx.Rows
	var info string
	if c.Request().URL.Path == "/eligible_pieces/sp_local" {

		info = strings.Join([]string{
			fmt.Sprintf(`List of qualifying Piece CIDs currently available within SPS %s itself`, sp.String()),
			``,
			`This list is ordered by most recently expiring/expired first, and reflects all pieces of data`,
			`that are still present within your own SP. It is recommended you perform these renewals first,`,
			`as data for them is readily obtainable.`,
			``,
			commonInfoFooter,
		}, "\n")

		rows, err = common.Db.Query(
			ctx,
			`
			WITH
				providers_in_org AS (
					SELECT provider_id FROM providers WHERE org_id IN ( SELECT city FROM providers WHERE provider_id = $1 )
				)
			SELECT
					d.dataset_slug,
					d.padded_size,
					d.piece_cid,
					d.deal_id,
					d.original_payload_cid,
					d.normalized_payload_cid,
					d.provider_id,
					d.is_fil_plus,
					d.end_time,
					NULL,
					NULL
				FROM deallist_eligible d
			WHERE

				d.provider_id = $1

					AND

				d.end_time < expiration_cutoff()

					AND

				-- I do not hold a better deal
				NOT EXISTS (
					SELECT 42
						FROM published_deals pd
					WHERE
						 pd.piece_cid = d.piece_cid
							AND
						pd.provider_id = $1
							AND
						pd.status != 'terminated'
							AND
						pd.end_time > d.end_time
				)

					AND

				-- the limit of active nonexpiring + in-fight deals within my org is not violated
				max_per_org() > (
					(
						SELECT COUNT(*)
							FROM published_deals pd
							JOIN clients c USING ( client_id )
							JOIN providers_in_org USING ( provider_id )
						WHERE
							pd.piece_cid = d.piece_cid
								AND
							c.is_affiliated
								AND
							pd.status = 'active'
								AND
							pd.end_time > expiration_cutoff()
					)
						+
					(
						SELECT COUNT(*)
							FROM proposals pr
							JOIN providers_in_org USING ( provider_id )
						WHERE
							pr.piece_cid = d.piece_cid
								AND
							pr.proposal_failstamp = 0
								AND
							pr.activated_deal_id IS NULL
					)
				)
			`,
			sp.String(),
		)
	} else {
		info = strings.Join([]string{
			`List of qualifying Piece CIDs together with their availability from various sources.`,
			``,
			`In order to satisfy a FilPlus deal from the evergreen engine, all you need to do is obtain the `,
			`corresponding .car file (usually by retrieving it from one of the sources within this list).`,
			``,
			commonInfoFooter,
		}, "\n")

		err = common.Db.QueryRow(
			ctx,
			`SELECT
					org_id,
					city,
					country,
					continent,
					max_per_org(),
					max_per_city(),
					max_per_country(),
					max_per_continent(),
					max_program_replicas()
				FROM providers
			WHERE provider_id = $1`,
			sp.String(),
		).Scan(&spOrgID, &spCity, &spCountry, &spContinent, &maxPerOrg, &maxPerCity, &maxPerCountry, &maxPerContinent, &programMax)
		if err != nil {
			return err
		}

		rows, err = common.Db.Query(
			ctx,
			`
			SELECT
					d.dataset_slug,
					d.padded_size,
					d.piece_cid,
					d.deal_id,
					d.original_payload_cid,
					d.normalized_payload_cid,
					d.provider_id,
					d.is_fil_plus,
					d.end_time,
					rc.counts AS counts_replicas,
					pc.counts AS counts_pending
				FROM deallist_eligible d
				JOIN counts_replicas rc USING ( piece_cid )
				JOIN counts_pending pc USING ( piece_cid )
			WHERE

				-- exclude my own in-flight proposals / actives
				NOT EXISTS (
					SELECT 42
						FROM proposals pr
					WHERE
						pr.piece_cid = d.piece_cid
							AND
						pr.proposal_failstamp = 0
							AND
						pr.activated_deal_id IS NULL
							AND
						pr.provider_id = $1
				)

					AND

				NOT EXISTS (
					SELECT 42
						FROM published_deals pd
					WHERE
						pd.piece_cid = d.piece_cid
							AND
						pd.status != 'terminated'
							AND
						pd.provider_id = $1
				)
			`,
			sp.String(),
		)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	// log.Info("queried")

	type pieceSpCombo struct {
		pcid string
		spid string
	}

	type aggCounts map[string]map[string]int64

	pieces := make(map[string]*piece, 1024)
	seenPieceSpCombo := make(map[pieceSpCombo]int64, 32768)
	ineligiblePcids := make(map[string]struct{}, 2048)
	for rows.Next() {
		s := filSource{SourceType: "Filecoin"}
		var p piece
		var rOriginal, rNormalized string
		var repCountsJSON, propCountsJSON *string

		if err = rows.Scan(&p.Dataset, &p.PaddedPieceSize, &p.PieceCid, &s.DealID, &rOriginal, &rNormalized, &s.ProviderID, &s.IsFilplus, &s.DealExpiration, &repCountsJSON, &propCountsJSON); err != nil {
			return err
		}

		if prevDealID, seen := seenPieceSpCombo[pieceSpCombo{pcid: p.PieceCid, spid: s.ProviderID}]; seen {
			return xerrors.Errorf("Unexpected double-deal for same sp/pcid: %d and %d", prevDealID, s.DealID)
		}
		seenPieceSpCombo[pieceSpCombo{pcid: p.PieceCid, spid: s.ProviderID}] = s.DealID

		if _, ineligible := ineligiblePcids[p.PieceCid]; ineligible {
			continue
		}

		if _, seen := pieces[p.PieceCid]; !seen {

			if repCountsJSON != nil {
				var active, proposed aggCounts
				if err := json.Unmarshal([]byte(*repCountsJSON), &active); err != nil {
					return err
				}
				if err := json.Unmarshal([]byte(*propCountsJSON), &proposed); err != nil {
					return err
				}

				if programMax <= active["total"]["total"]+proposed["total"]["total"] ||
					maxPerOrg <= active["org_id"][spOrgID]+proposed["org_id"][spOrgID] ||
					maxPerCity <= active["city"][spCity]+proposed["city"][spCity] ||
					maxPerCountry <= active["country"][spCountry]+proposed["country"][spCountry] ||
					maxPerContinent <= active["continent"][spContinent]+proposed["continent"][spContinent] {

					ineligiblePcids[p.PieceCid] = struct{}{}
					continue
				}
			}

			p.PayloadCids = append(p.PayloadCids, rNormalized)
			p.SampleRequestCmd = fmt.Sprintf(
				`echo curl -sLH "Authorization: $( ./fil-spid.bash %s )" https://api.evergreen.filecoin.io/request_piece/%s | sh`,
				sp.String(),
				p.PieceCid,
			)
			pieces[p.PieceCid] = &p
		}

		s.SampleRetrieveCmd = fmt.Sprintf(
			"lotus client retrieve --provider %s --maxPrice 0 --allow-local --car '%s' %s__%s.car",
			s.ProviderID,
			rOriginal,
			common.TrimCidString(p.PieceCid),
			common.TrimCidString(rNormalized),
		)

		s.sysIDStr = fmt.Sprintf("%d", s.DealID)
		s.expUnixNano = s.DealExpiration.UnixNano()
		s.expCoarse = s.DealExpiration.Truncate(time.Hour * 24 * 7).UnixNano()

		pieces[p.PieceCid].Sources = append(pieces[p.PieceCid].Sources, &s)
	}

	ret := make([]*piece, 0, 2048)
	for _, p := range pieces {
		sort.Slice(p.Sources, func(i, j int) bool {
			switch {

			case p.Sources[i].srcType() != p.Sources[j].srcType():
				return p.Sources[i].srcType() < p.Sources[j].srcType()

			case p.Sources[i].expiryUnixNano() != p.Sources[j].expiryUnixNano():
				return p.Sources[i].expiryUnixNano() > p.Sources[j].expiryUnixNano()

			default:
				return p.Sources[i].sysID() < p.Sources[j].sysID()
			}
		})
		ret = append(ret, p)
	}

	// log.Info("pulled")

	sort.Slice(ret, func(i, j int) bool {
		si, sj := ret[i].Sources[len(ret[i].Sources)-1], ret[j].Sources[len(ret[j].Sources)-1]

		switch {

		case si.expiryCoarse() != sj.expiryCoarse():
			return si.expiryCoarse() < sj.expiryCoarse()

		default:
			return ret[i].PieceCid < ret[j].PieceCid

		}
	})

	if uint64(len(ret)) > lim {
		info = strings.Join([]string{
			info,
			``,
			fmt.Sprintf(`NOTE: The complete list of %d entries has been TRUNCATED to the top %d.`, len(ret), lim),
			fmt.Sprintf(`You can add the ...?limit=%d parameter to your call to see the full (possibly very large) list.`, len(ret)),
		}, "\n")
		ret = ret[:lim]
	}

	// log.Info("sorted")

	return retPayloadAnnotated(c, http.StatusOK, ret, info)
}

func (s *filSource) srcType() string       { return "Filecoin" }
func (s *filSource) expiryCoarse() int64   { return s.expCoarse }
func (s *filSource) expiryUnixNano() int64 { return s.expUnixNano }
func (s *filSource) sysID() string         { return s.sysIDStr }
