module github.com/filecoin-project/evergreen-dealer

go 1.19

replace github.com/filecoin-project/evergreen-dealer/webapi/types => ./webapi/types

// temporarily ensure no boost dep creeps in
replace github.com/filecoin-project/boost => /dev/null

require (
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dustin/go-humanize v1.0.0
	github.com/filecoin-project/evergreen-dealer/webapi/types v0.0.0
	github.com/filecoin-project/go-address v1.1.0
	github.com/filecoin-project/go-cbor-util v0.0.1
	github.com/filecoin-project/go-fil-markets v1.25.0
	github.com/filecoin-project/go-jsonrpc v0.1.9
	github.com/filecoin-project/go-state-types v0.9.9
	github.com/filecoin-project/lotus v1.18.0
	github.com/georgysavva/scany v1.2.1
	github.com/google/uuid v1.3.0
	github.com/hashicorp/golang-lru/v2 v2.0.1
	github.com/ipfs/go-cid v0.3.2
	github.com/ipfs/go-fs-lock v0.0.7
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/jackc/pgx/v4 v4.17.2
	github.com/labstack/echo/v4 v4.9.1
	github.com/mattn/go-isatty v0.0.16
	github.com/multiformats/go-multibase v0.1.1
	github.com/multiformats/go-multihash v0.2.1
	github.com/prometheus/client_golang v1.14.0
	github.com/urfave/cli/v2 v2.23.5
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.2.0
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
)

require (
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/DataDog/zstd v1.4.1 // indirect
	github.com/GeertJohan/go.incremental v1.0.0 // indirect
	github.com/GeertJohan/go.rice v1.0.2 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/akavel/rsrc v0.8.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20170627025303-887ab5e44cc3 // indirect
	github.com/daaku/go.zipexe v1.0.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/filecoin-project/dagstore v0.5.5 // indirect
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v4 v4.0.0 // indirect
	github.com/filecoin-project/go-bitfield v0.2.4 // indirect
	github.com/filecoin-project/go-data-transfer v1.15.2 // indirect
	github.com/filecoin-project/go-hamt-ipld v0.1.5 // indirect
	github.com/filecoin-project/go-hamt-ipld/v2 v2.0.0 // indirect
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-padreader v0.0.1 // indirect
	github.com/filecoin-project/go-statestore v0.2.0 // indirect
	github.com/filecoin-project/specs-actors v0.9.15 // indirect
	github.com/filecoin-project/specs-actors/v2 v2.3.6 // indirect
	github.com/filecoin-project/specs-actors/v3 v3.1.2 // indirect
	github.com/filecoin-project/specs-actors/v4 v4.0.2 // indirect
	github.com/filecoin-project/specs-actors/v5 v5.0.6 // indirect
	github.com/filecoin-project/specs-actors/v6 v6.0.2 // indirect
	github.com/filecoin-project/specs-actors/v7 v7.0.1 // indirect
	github.com/gbrlsnchs/jwt/v3 v3.0.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hannahhoward/cbor-gen-for v0.0.0-20200817222906-ea96cece81f1 // indirect
	github.com/hannahhoward/go-pubsub v0.0.0-20200423002714-8d62886cc36e // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/icza/backscanner v0.0.0-20210726202459-ac2ffc679f94 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-block-format v0.0.3 // indirect
	github.com/ipfs/go-blockservice v0.4.0 // indirect
	github.com/ipfs/go-datastore v0.6.0 // indirect
	github.com/ipfs/go-graphsync v0.13.1 // indirect
	github.com/ipfs/go-ipfs-blockstore v1.2.0 // indirect
	github.com/ipfs/go-ipfs-cmds v0.7.0 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.0 // indirect
	github.com/ipfs/go-ipfs-exchange-interface v0.2.0 // indirect
	github.com/ipfs/go-ipfs-files v0.1.1 // indirect
	github.com/ipfs/go-ipfs-http-client v0.4.0 // indirect
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-ipld-cbor v0.0.6 // indirect
	github.com/ipfs/go-ipld-format v0.4.0 // indirect
	github.com/ipfs/go-ipld-legacy v0.1.1 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-merkledag v0.8.0 // indirect
	github.com/ipfs/go-metrics-interface v0.0.1 // indirect
	github.com/ipfs/go-path v0.3.0 // indirect
	github.com/ipfs/go-unixfs v0.3.1 // indirect
	github.com/ipfs/go-verifcid v0.0.1 // indirect
	github.com/ipfs/interface-go-ipfs-core v0.7.0 // indirect
	github.com/ipld/go-car v0.4.1-0.20220707083113-89de8134e58e // indirect
	github.com/ipld/go-codec-dagpb v1.3.2 // indirect
	github.com/ipld/go-ipld-prime v0.18.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.13.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.1 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.12.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/labstack/gommon v0.4.0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.1.0 // indirect
	github.com/libp2p/go-libp2p v0.23.4 // indirect
	github.com/libp2p/go-libp2p-core v0.20.0 // indirect
	github.com/libp2p/go-libp2p-pubsub v0.8.0 // indirect
	github.com/libp2p/go-msgio v0.2.0 // indirect
	github.com/libp2p/go-openssl v0.1.0 // indirect
	github.com/magefile/mage v1.9.0 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.50 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multiaddr v0.8.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.3.1 // indirect
	github.com/multiformats/go-multicodec v0.6.0 // indirect
	github.com/multiformats/go-varint v0.0.6 // indirect
	github.com/nkovacs/streamquote v1.0.0 // indirect
	github.com/opencontainers/runtime-spec v1.0.3-0.20210326190908-1c3f411f0417 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/polydawn/refmt v0.0.0-20201211092308-30ac6d18308e // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shirou/gopsutil v2.18.12+incompatible // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/whyrusleeping/bencher v0.0.0-20190829221104-bb6607aa8bba // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20220514204315-f29c37e9c44c // indirect
	github.com/whyrusleeping/timecache v0.0.0-20160911033111-cfcb2f1abfee // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/otel v1.9.0 // indirect
	go.opentelemetry.io/otel/sdk v1.9.0 // indirect
	go.opentelemetry.io/otel/trace v1.9.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.23.0 // indirect
	go4.org v0.0.0-20200411211856-f5505b9728dd // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.0.0-20220920183852-bf014ff85ad5 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	golang.org/x/tools v0.1.12 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
)
