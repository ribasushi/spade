package main

import (
	"context"
	"fmt"
	"time"

	cmn "github.com/filecoin-project/evergreen-dealer/common"
	"github.com/filecoin-project/evergreen-dealer/infomempeerstore"
	cborutil "github.com/filecoin-project/go-cbor-util"
	lotusbuild "github.com/filecoin-project/lotus/build"
	"github.com/jackc/pgx/v4"
	lp2p "github.com/libp2p/go-libp2p"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	lp2pnet "github.com/libp2p/go-libp2p/core/network"
	lp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	lp2pproto "github.com/libp2p/go-libp2p/core/protocol"
	lp2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	lp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	lp2ptcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func newLp2pNode(withTimeout time.Duration) (lp2phost.Host, *infomempeerstore.PeerStore, error) {
	ps, err := infomempeerstore.NewPeerstore()
	if err != nil {
		return nil, nil, cmn.WrErr(err)
	}

	connmgr, err := lp2pconnmgr.NewConnManager(8192, 16384) // effectively deactivate
	if err != nil {
		return nil, nil, cmn.WrErr(err)
	}

	nodeHost, err := lp2p.New(
		lp2p.Peerstore(ps),  // allows us collect random on-connect data
		lp2p.RandomIdentity, // *NEVER* reuse a peerid
		lp2p.DisableRelay(),
		lp2p.ResourceManager(lp2pnet.NullResourceManager),
		lp2p.ConnectionManager(connmgr),
		lp2p.Ping(false),
		lp2p.NoListenAddrs,
		lp2p.NoTransports,
		lp2p.Transport(lp2ptcp.NewTCPTransport, lp2ptcp.WithConnectionTimeout(withTimeout+100*time.Millisecond)),
		lp2p.Security(lp2ptls.ID, lp2ptls.New),
		lp2p.UserAgent("lotus-"+lotusbuild.BuildVersion+lotusbuild.BuildTypeString()),
		lp2p.WithDialTimeout(withTimeout),
	)
	if err != nil {
		return nil, nil, cmn.WrErr(err)
	}

	return nodeHost, ps, nil
}

func lp2pRPC(
	ctx context.Context,
	host lp2phost.Host, peer lp2ppeer.ID,
	proto lp2pproto.ID, args interface{}, resp interface{},
) error {

	st, err := host.NewStream(ctx, peer, proto)
	if err != nil {
		return fmt.Errorf("error while opening %s stream: %w", proto, err)
	}
	defer st.Close() //nolint:errcheck
	if d, hasDline := ctx.Deadline(); hasDline {
		st.SetDeadline(d)                 //nolint:errcheck
		defer st.SetDeadline(time.Time{}) //nolint:errcheck
	}
	if args != nil {
		if err := cborutil.WriteCborRPC(st, args); err != nil {
			return fmt.Errorf("error while writing to %s stream: %w", proto, err)
		}
	}

	if err := cborutil.ReadCborRPC(st, resp); err != nil {
		return fmt.Errorf("error while reading %s response: %w", proto, err)
	}

	return nil
}

func refreshMatviews(ctx context.Context, tx pgx.Tx) error {
	// refresh matviews
	log.Info("refreshing materialized views")
	for _, mv := range []string{
		"mv_deals_prefiltered_for_repcount", "mv_orglocal_presence",
		"mv_replicas_continent", "mv_replicas_org", "mv_replicas_city", "mv_replicas_country",
		"mv_overreplicated_city", "mv_overreplicated_country", "mv_overreplicated_total", "mv_overreplicated_continent", "mv_overreplicated_org",
		"mv_pieces_availability",
	} {
		t0 := time.Now()
		if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY egd.`+mv); err != nil {
			return cmn.WrErr(err)
		}
		if _, err := tx.Exec(ctx, `ANALYZE egd.`+mv); err != nil {
			return cmn.WrErr(err)
		}
		log.Infow("refreshed", "view", mv, "took_seconds", time.Since(t0).Truncate(time.Millisecond).Seconds())
	}

	return nil
}
