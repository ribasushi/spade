package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	prometheuspush "github.com/prometheus/client_golang/prometheus/push"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

type metricType string

var (
	metricGauge   = metricType("gauge")
	metricCounter = metricType("counter")

	metricDbTimeout = 30 * time.Minute
	workerCount     = 24
)

type metricSpec struct {
	kind  metricType
	name  string
	help  string
	query string
}

type metricResult struct {
	metricSpec
	value  int64
	labels prometheus.Labels
}

var pushMetrics = &cli.Command{
	Usage:  "Push service metrics to external collectors",
	Name:   "push-metrics",
	Flags:  []cli.Flag{},
	Action: pushPrometheusMetrics,
}

var metricsList = []metricSpec{
	{
		kind:  metricGauge,
		name:  "clients_datacap",
		help:  "Amount of datacap remaining per affiliated client address",
		query: `SELECT client_address, datacap_available FROM clients_datacap_available`,
	},
	{
		kind: metricGauge,
		name: "active_deals",
		help: "Count of filecoin deals made by service",
		query: `
			SELECT
					v.status,
					(
						SELECT COUNT(*)
							FROM published_deals pd
							JOIN clients cl USING ( client_id )
							JOIN pieces p USING ( piece_cid )
						WHERE
							pd.status = v.status
								AND
							cl.is_affiliated
								AND
							NOT COALESCE( (p.meta->'inactive')::BOOL, false )
								AND
							NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
					)
				FROM ( VALUES ('published'), ('active'), ('terminated') ) AS v (status)
		`,
	},
	{
		kind: metricGauge,
		name: "active_deals_piecebytes",
		help: "Raw padded-piece-size of active filecoin deals made by service",
		query: `
			SELECT
					SUM( p.padded_size )
				FROM published_deals pd
				JOIN clients cl USING ( client_id )
				JOIN pieces p USING ( piece_cid )
			WHERE
				cl.is_affiliated
					AND
				pd.status = 'active'
					AND
				NOT COALESCE( (p.meta->'inactive')::BOOL, false )
					AND
				NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
		`,
	},
	{
		kind: metricGauge,
		name: "active_deals_piecebytes_per_continent",
		help: "Raw padded-piece-size of active filecoin deals made by service per continent",
		query: `
			SELECT
					c.continent,
					COALESCE( (
						SELECT SUM( p.padded_size )
							FROM published_deals pd
							JOIN clients cl USING ( client_id )
							JOIN providers pr USING ( provider_id )
							JOIN pieces p USING ( piece_cid )
						WHERE
							pr.continent = c.continent
								AND
							cl.is_affiliated
								AND
							pd.status = 'active'
								AND
							NOT COALESCE( (p.meta->'inactive')::BOOL, false )
								AND
							NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
								), 0)
				FROM continents() c
		`,
	},
	{
		kind: metricGauge,
		name: "active_deals_piecebytes_per_spid",
		help: "Raw padded-piece-size of active filecoin deals made by service per storage provider",
		query: `
			SELECT
					pd.provider_id,
					SUM( p.padded_size )
				FROM published_deals pd
				JOIN clients cl USING ( client_id )
				JOIN providers pr USING ( provider_id )
				JOIN pieces p USING ( piece_cid )
			WHERE
				cl.is_affiliated
					AND
				pd.status = 'active'
					AND
				NOT COALESCE( (p.meta->'inactive')::BOOL, false )
					AND
				NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
			GROUP BY
				pd.provider_id
		`,
	},
	{
		kind: metricGauge,
		name: "dataset_unique_piecebytes",
		help: "Unique padded piece sizes of entries per dataset",
		query: `
			SELECT
					d.dataset_slug, SUM( p.padded_size )
				FROM datasets d
				JOIN pieces p USING ( dataset_id )
			GROUP BY d.dataset_slug
		`,
	},
	{
		kind: metricGauge,
		name: "dataset_unique_piecebytes_on_chain_legacy",
		help: "Unique padded piece sizes of entries with at least one active on chain deal per dataset group made outside of service",
		query: `
			WITH
				onchain AS (
					SELECT p.dataset_id, SUM(p.padded_size) sz
						FROM pieces p
					WHERE
						EXISTS (
							SELECT 42
								FROM published_deals pd
								JOIN clients c USING ( client_id )
							WHERE
								p.piece_cid = pd.piece_cid
									AND
								pd.status = 'active'
									AND
								NOT COALESCE( (p.meta->'inactive')::BOOL, false )
									AND
								NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
									AND
								NOT c.is_affiliated
							)
							AND
						NOT EXISTS (
							SELECT 42
								FROM published_deals pd
								JOIN clients c USING ( client_id )
							WHERE
								p.piece_cid = pd.piece_cid
									AND
								pd.status = 'active'
									AND
								NOT COALESCE( (p.meta->'inactive')::BOOL, false )
									AND
								NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
									AND
								c.is_affiliated
						)
					GROUP BY p.dataset_id
				)
			SELECT
					d.dataset_slug, COALESCE( onchain.sz, 0 )
				FROM datasets d
				JOIN onchain USING ( dataset_id )
		`,
	},
	{
		kind: metricGauge,
		name: "dataset_unique_piecebytes_on_chain_service",
		help: "Unique padded piece sizes of entries with at least one active on chain deal per dataset group made by service",
		query: `
			WITH
				onchain AS (
					SELECT p.dataset_id, SUM(p.padded_size) sz
						FROM pieces p
					WHERE
						EXISTS (
							SELECT 42
								FROM published_deals pd
								JOIN clients c USING ( client_id )
							WHERE
								p.piece_cid = pd.piece_cid
									AND
								pd.status = 'active'
									AND
								c.is_affiliated
									AND
								NOT COALESCE( (p.meta->'inactive')::BOOL, false )
									AND
								NOT COALESCE( (pd.meta->'inactive')::BOOL, false )
						)
					GROUP BY p.dataset_id
				)
			SELECT
					d.dataset_slug, COALESCE( onchain.sz, 0 )
				FROM datasets d
				JOIN onchain USING ( dataset_id )
		`,
	},
}

func pushPrometheusMetrics(cctx *cli.Context) error {

	var countPromCounters, countPromGauges int
	defer func() {
		log.Infow("prometheus push completed",
			"counterMetrics", countPromCounters,
			"gaugeMetrics", countPromGauges,
		)
	}()

	jobQueue := make(chan metricSpec, len(metricsList))
	for _, m := range metricsList {
		jobQueue <- m
	}
	close(jobQueue)

	res := make([]metricResult, 0, 1024)
	doneCh := make(chan struct{}, workerCount)
	var firstErrorSeen error
	var mu sync.Mutex
	for i := 0; i < workerCount; i++ {
		go func() {
			defer func() { doneCh <- struct{}{} }()

			for {
				m, chanOpen := <-jobQueue
				if !chanOpen {
					return
				}
				r, err := gatherMetric(cctx, m)

				mu.Lock()
				if err != nil {
					log.Errorf("failed gathering data for %s, continuing nevertheless: %s ", m.name, err)
					if firstErrorSeen == nil {
						firstErrorSeen = err
					}
				} else {
					res = append(res, r...)
				}
				mu.Unlock()

			}
		}()
	}

	for workerCount > 0 {
		<-doneCh
		workerCount--
	}

	prom := prometheuspush.New(cmn.PromURL, cmn.NonAlphanumRun.ReplaceAllString(cmn.AppName+"_metrics", "_")).
		Grouping("instance", cmn.NonAlphanumRun.ReplaceAllString(cmn.PromInstance, "_")).
		BasicAuth(cmn.PromUser, cmn.PromPass)

	for _, r := range res {
		if r.kind == metricCounter {
			countPromCounters++
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: r.name, Help: r.help, ConstLabels: r.labels})
			c.Add(float64(r.value))
			prom.Collector(c)
		} else if r.kind == metricGauge {
			countPromGauges++
			c := prometheus.NewGauge(prometheus.GaugeOpts{Name: r.name, Help: r.help, ConstLabels: r.labels})
			c.Set(float64(r.value))
			prom.Collector(c)
		} else {
			return xerrors.Errorf("unknown metric kind '%s'", r.kind)
		}
	}

	if err := prom.Push(); err != nil {
		return cmn.WrErr(err)
	}

	return firstErrorSeen
}

func gatherMetric(cctx *cli.Context, m metricSpec) ([]metricResult, error) {

	ctx := cctx.Context
	t0 := time.Now()

	var metricTx pgx.Tx
	defer func() {
		if metricTx != nil {
			metricTx.Rollback(context.Background()) //nolint:errcheck
		}
	}()

	msecTOut := metricDbTimeout.Milliseconds()
	if metricsConnStr := cctx.String("pg-metrics-connstring"); metricsConnStr != "" {
		metricDb, err := pgx.Connect(ctx, metricsConnStr)
		if err != nil {
			return nil, cmn.WrErr(err)
		}
		defer metricDb.Close(context.Background()) //nolint:errcheck

		// separate db - means we can have a connection-wide timeout
		if _, err = metricDb.Exec(ctx, fmt.Sprintf(`SET statement_timeout = %d`, msecTOut)); err != nil {
			return nil, cmn.WrErr(err)
		}
		metricTx, err = metricDb.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, cmn.WrErr(err)
		}
	} else {
		var err error
		if metricTx, err = cmn.Db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}); err != nil {
			return nil, cmn.WrErr(err)
		}
		// using wider DB - must be tx-local timeout
		if _, err = metricTx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = %d`, msecTOut)); err != nil {
			return nil, cmn.WrErr(err)
		}
	}

	rows, err := metricTx.Query(ctx, m.query)
	if err != nil {
		return nil, cmn.WrErr(err)
	}
	defer rows.Close()

	colnames := make([]string, 0, 2)
	for _, f := range rows.FieldDescriptions() {
		colnames = append(colnames, string(f.Name))
	}

	if len(colnames) < 1 || len(colnames) > 2 {
		return nil, xerrors.Errorf("unexpected %d columns in resultset", len(colnames))
	}

	res := make(map[string]*int64)

	if len(colnames) == 1 {

		if !rows.Next() {
			return nil, xerrors.New("zero rows in result")
		}

		var val *int64
		if err := rows.Scan(&val); err != nil {
			return nil, cmn.WrErr(err)
		}

		if rows.Next() {
			return nil, xerrors.New("unexpectedly received more than one result")
		}
		res[""] = val

	} else {

		for rows.Next() {
			var group string
			var val *int64
			if err := rows.Scan(&group, &val); err != nil {
				return nil, cmn.WrErr(err)
			}
			res[group] = val
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, cmn.WrErr(err)
	}
	took := time.Since(t0).Truncate(time.Millisecond).Seconds()

	metricTx.Rollback(context.Background()) //nolint:errcheck
	metricTx = nil

	ret := make([]metricResult, 0, len(res))
	for g, v := range res {

		dims := append(
			make([][2]string, 0, 2),
			[2]string{"instance", cmn.NonAlphanumRun.ReplaceAllString(cmn.PromInstance, "_")},
		)

		labels := make(prometheus.Labels)
		if g != "" {
			gType := colnames[0]
			labels[gType] = g
			dims = append(dims, [2]string{gType, g})
		}

		if m.kind == metricCounter || m.kind == metricGauge {
			log.Infow(string(m.kind)+"Evaluated", "name", m.name, "labels", labels, "value", v, "tookSeconds", took)
		} else {
			return nil, xerrors.Errorf("unknown metric kind '%s'", m.kind)
		}

		if v != nil {
			ret = append(
				ret,
				metricResult{
					metricSpec: m,
					value:      *v,
					labels:     labels,
				},
			)
		}

		_, err = cmn.Db.Exec(
			ctx,
			`
			INSERT INTO metrics
					( name, dimensions, description, value, collection_took_seconds )
				VALUES
					( $1, $2, $3, $4, $5 )
				ON CONFLICT ( name, dimensions ) DO UPDATE SET
					description = EXCLUDED.description,
					value = EXCLUDED.value,
					collection_took_seconds = EXCLUDED.collection_took_seconds,
					collected_at = CLOCK_TIMESTAMP()
			`,
			m.name,
			dims,
			m.help,
			v,
			took,
		)
		if err != nil {
			return nil, cmn.WrErr(err)
		}
	}

	return ret, nil
}
