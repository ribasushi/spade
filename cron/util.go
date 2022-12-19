package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/spade/internal/app"
)

func refreshMatviews(ctx context.Context, tx pgx.Tx) error {
	log := app.GetGlobalCtx(ctx).Logger

	// refresh matviews
	log.Info("refreshing materialized views")
	for _, mv := range []string{
		"mv_deals_prefiltered_for_repcount", "mv_orglocal_presence",
		"mv_replicas_continent", "mv_replicas_org", "mv_replicas_city", "mv_replicas_country",
		"mv_overreplicated_city", "mv_overreplicated_country", "mv_overreplicated_total", "mv_overreplicated_continent", "mv_overreplicated_org",
		"mv_pieces_availability",
	} {
		t0 := time.Now()
		if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY spd.`+mv); err != nil {
			return cmn.WrErr(err)
		}
		if _, err := tx.Exec(ctx, `ANALYZE spd.`+mv); err != nil {
			return cmn.WrErr(err)
		}
		log.Infow("refreshed", "view", mv, "took_seconds", time.Since(t0).Truncate(time.Millisecond).Seconds())
	}

	return nil
}
