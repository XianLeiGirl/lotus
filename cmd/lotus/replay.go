package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dtynn/dix"
	amt4 "github.com/filecoin-project/go-amt-ipld/v4"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/beacon"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/journal/fsjournal"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	metricsi "github.com/ipfs/go-metrics-interface"
	"github.com/urfave/cli/v2"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/fx"
	"net/http"
)

const (
	invokeNone dix.Invoke = iota // nolint: varcheck,deadcode

	invokeSetupGrafana
	invokeSetupDebug
	invokeSetupMetrics
	invokeSetupTracing

	invokePopulate
)

var replayCmd = &cli.Command{
	Name:  "replay",
	Usage: "replay chain data to dst repo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "src-repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst-repo",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "start-height",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "end-ts",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.Background()
		var components struct {
			fx.In
			CS  *store.ChainStore
			Stm *stmgr.StateManager
		}

		stopper, err := dix.New(ctx, Reject(cctx, &components))
		defer stopper(ctx)
		if err != nil {
			return err
		}

		r, err := repo.NewFS(cctx.String("src-repo"))
		if err != nil {
			return fmt.Errorf("opening fs repo: %w", err)
		}

		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			return fmt.Errorf("src repo error: %w", err)
		}

		lr, err := r.Lock(repo.FullNode)
		if err != nil {
			return err
		}
		defer lr.Close() //nolint:errcheck

		bs, err := lr.Blockstore(cctx.Context, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		mds, err := lr.Datastore(context.TODO(), "/metadata")
		if err != nil {
			return err
		}

		j, err := fsjournal.OpenFSJournal(lr, journal.EnvDisabledEvents())
		if err != nil {
			return fmt.Errorf("failed to open journal: %w", err)
		}

		cst := store.NewChainStore(bs, bs, mds, filcns.Weight, j)
		cids, err := lcli.ParseTipSetString(cctx.String("end-ts"))
		if err != nil {
			return err
		}

		ts, err := cst.LoadTipSet(ctx, types.NewTipSetKey(cids...))
		if err != nil {
			return fmt.Errorf("importing chain failed: %w", err)
		}

		start := cctx.Int("start-height")
		log.Info("start, end height", start, ts.Height())

		tss := []*types.TipSet{}

		for ts.Height() >= abi.ChainEpoch(start) {
			tss = append(tss, ts)
			ts, err = cst.LoadTipSet(ctx, ts.Parents())
			if err != nil {
				return fmt.Errorf("load ts failed: %w", err)
			}
		}

		got := len(tss)
		for i := 0; i < got/2; i++ {
			j := got - i - 1
			tss[i], tss[j] = tss[j], tss[i]
		}

		log.Infof("components: %v", components)

		//replay
		for _, ts := range tss {
			_, ires, err := components.Stm.ExecutionTrace(ctx, ts)
			if err != nil {
				log.Error(err)
				return err
			}

			log.Infof("execute tipset %v, len(ires): %v", ts.Height(), len(ires))

			//validate
			for _, r := range ires {
				if r.MsgRct != nil && r.MsgRct.Version() == types.MessageReceiptV1 {
					eventsRoot := r.MsgRct.EventsRoot
					if eventsRoot != nil {
						events, err := LoadEvents(ctx, components.CS, *eventsRoot)
						if err != nil {
							log.Errorf("load events for root %v failed: %v", r.MsgRct.EventsRoot.String(), err)
							return err
						}

						log.Infof("load events for root %v at %v successfly, events: %v", r.MsgRct.EventsRoot.String(), ts.Height(), events)
					}

					log.Infof("null eventsRoot")
				}
			}
		}

		return nil
	},
}

func Reject(cctx *cli.Context, target ...interface{}) dix.Option {
	return dix.Options(
		dix.If(len(target) > 0, dix.Populate(invokePopulate, target...)),
		ContextModule(context.Background()),
		StateManager(),
		InjectChainRepo(cctx), OfflineDataSource(),
	)
}

type GlobalContext context.Context

func ContextModule(ctx context.Context) dix.Option {
	return dix.Options(dix.Override(new(GlobalContext), ctx),
		dix.Override(new(*http.ServeMux), http.NewServeMux()),
		dix.Override(new(helpers.MetricsCtx), metricsi.CtxScope(ctx, "bell")))
}

func StateManager() dix.Option {
	return dix.Options(dix.Override(new(vm.SyscallBuilder), vm.Syscalls),
		dix.Override(new(storiface.Verifier), ffiwrapper.ProofVerifier),
		dix.Override(new(journal.Journal), modules.OpenFilesystemJournal),
		dix.Override(new(journal.DisabledEvents), journal.EnvDisabledEvents),
		dix.Override(new(store.WeightFunc), filcns.Weight),
		dix.Override(new(*store.ChainStore), modules.ChainStore),
		dix.Override(new(stmgr.Executor), filcns.NewTipSetExecutor),
		dix.Override(new(dtypes.DrandSchedule), modules.BuiltinDrandConfig),
		dix.Override(new(dtypes.AfterGenesisSet), modules.SetGenesis),
		dix.Override(new(beacon.Schedule), modules.RandomSchedule),
		dix.Override(new(stmgr.UpgradeSchedule), modules.UpgradeSchedule),
		dix.Override(new(*stmgr.StateManager), stmgr.NewStateManager),
		dix.Override(new(modules.Genesis), modules.LoadGenesis(build.MaybeGenesis())),
		dix.Override(new(dtypes.ChainBlockstore), dix.From(new(dtypes.BasicChainBlockstore))),
		dix.Override(new(dtypes.StateBlockstore), dix.From(new(dtypes.BasicChainBlockstore))),
		dix.Override(new(dtypes.BaseBlockstore), dix.From(new(dtypes.BasicChainBlockstore))),
		dix.Override(new(dtypes.ExposedBlockstore), dix.From(new(dtypes.BasicChainBlockstore))))
}

func InjectChainRepo(cctx *cli.Context) dix.Option {
	return dix.Override(new(repo.LockedRepo), func(lc fx.Lifecycle) repo.LockedRepo {
		r, err := repo.NewFS(cctx.String("dst-repo"))
		if err != nil {
			panic(fmt.Errorf("opening fs repo: %w", err))
		}
		err = r.Init(repo.FullNode)
		if err != nil && err != repo.ErrRepoExists {
			panic(fmt.Errorf("dst repo error: %w", err))
		}

		lr, err := r.Lock(repo.FullNode)
		if err != nil {
			panic(fmt.Errorf("lock repo failed: %w", err))
		}

		return modules.LockedRepo(lr)(lc)
	})
}

func OfflineDataSource() dix.Option {
	return dix.Options(
		// Notice: we may need to use other datastore someday. It depends on
		// the origin data structs.
		dix.Override(new(dtypes.AfterGenesisSet), modules.SetGenesis),
		dix.Override(new(dtypes.BasicChainBlockstore), modules.UniversalBlockstore),
		dix.Override(new(dtypes.MetadataDS), modules.Datastore(true)),
	)
}

func LoadEvents(ctx context.Context, cs *store.ChainStore, eventsRoot cid.Cid) ([]types.Event, error) {
	store := cs.ActorStore(ctx)
	amt, err := amt4.LoadAMT(ctx, store, eventsRoot, amt4.UseTreeBitWidth(types.EventAMTBitwidth))
	if err != nil {
		return nil, err
	}

	ret := make([]types.Event, 0, amt.Len())
	err = amt.ForEach(ctx, func(u uint64, deferred *cbg.Deferred) error {
		var evt types.Event
		if err := evt.UnmarshalCBOR(bytes.NewReader(deferred.Raw)); err != nil {
			return err
		}
		ret = append(ret, evt)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
