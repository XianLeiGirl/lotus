package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-force-community/londobell/racailum/segment/model"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/hashicorp/go-multierror"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dtynn/dix"
	amt4 "github.com/filecoin-project/go-amt-ipld/v4"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/fx"

	"github.com/ipfs-force-community/londobell/lib/limiter"

	"github.com/ipfs-force-community/londobell/dep"
)

var replayCmd = &cli.Command{
	Name: "replay",
	Subcommands: []*cli.Command{
		eventsRootCmd,
		actorEventCmd,
	},
}

var eventsRootCmd = &cli.Command{
	Name:  "events-root",
	Usage: "replay chain data to repo",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "start-height",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "end-ts",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "name",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "chain-storage-repo",
			Usage: "repo path of chain storage",
		},
		&cli.Int64SliceFlag{
			Name: "skip-heights",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.Background()

		skipHeights := cctx.Int64Slice("skip-heights")
		log.Infof("skipHeights: %v", skipHeights)

		var components struct {
			fx.In
			CS  *store.ChainStore
			Stm *stmgr.StateManager
		}

		stopper, err := dix.New(ctx,
			Inject(cctx, &components),
		)

		defer stopper(ctx) //nolint:errcheck

		if err != nil {
			return err
		}

		components.CS.StoreEvents(true)
		log.Infof("IsStoringEvents: %v", components.Stm.ChainStore().IsStoringEvents())

		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(cctx.String("dsn")))
		if err != nil {
			return err
		}

		db := client.Database(cctx.String("name"))
		eventsRootCol := db.Collection("EventsRoot")

		cids, err := lcli.ParseTipSetString(cctx.String("end-ts"))
		if err != nil {
			return err
		}

		ts, err := components.CS.LoadTipSet(ctx, types.NewTipSetKey(cids...))
		if err != nil {
			return fmt.Errorf("importing chain failed: %w", err)
		}

		start := cctx.Int("start-height")

		log.Infof("start: %v, end: %v", start, ts.Height())

		tss := []*types.TipSet{}

		for ts.Height() >= abi.ChainEpoch(start) {
			tss = append(tss, ts)
			ts, err = components.CS.LoadTipSet(ctx, ts.Parents())
			if err != nil {
				return fmt.Errorf("load ts failed: %w", err)
			}
		}

		got := len(tss)
		for i := 0; i < got/2; i++ {
			j := got - i - 1
			tss[i], tss[j] = tss[j], tss[i]
		}

		var (
			parts  [][]*types.TipSet
			size   = len(tss)
			tsDone = 0
			gap    = 120
		)

		for tsDone < size {
			start := tsDone
			end := start + gap
			if end > size {
				end = size
			}

			part := tss[start:end]
			parts = append(parts, part)
			tsDone += len(part)
		}

		log.Infof("len(tss): %v", size)

		lim := limiter.New(16)
		var ewg multierror.Group

		var (
			doneCount = 0
			lk        sync.Mutex
		)

		for _, part := range parts {
			part := part
			if len(part) == 0 {
				continue
			}

			alog := log.With("range", fmt.Sprintf("[%v, %v]", part[0].Height(), part[len(part)-1].Height()))

			starttime := time.Now()
			alog.Infof("[%v, %v] part begin", part[0].Height(), part[len(part)-1].Height())

			for _, ts := range part {
				ts := ts

				ewg.Go(func() error {
					if !lim.Acquire(context.TODO()) {
						return nil
					}

					defer func() {
						lim.Release(context.TODO())
					}()

					alog.Infof("begin tipset %v", ts.Height())
					start := time.Now()
					skip := false
					for _, h := range skipHeights {
						if h == int64(ts.Height()) {
							skip = true
							break
						}
					}

					if skip {
						alog.Infof("skip tipset %v for existing in skipHeights", ts.Height())

						lk.Lock()
						doneCount++
						alog.Infof("handle tipset %v successfully, %v/%v", ts.Height(), doneCount, len(tss))
						lk.Unlock()

						return nil
					}

					starttime := time.Now()
					cmsgs, err := components.CS.MessagesForTipset(ctx, ts)
					if err != nil {
						alog.Error(err)
						return err
					}

					alog.Infof("get messages for tipset %v, elapsed: %v", ts.Height(), time.Now().Sub(starttime).String())

					starttime = time.Now()
					var exist = true
					//for _, cmsg := range cmsgs {
					//	if smsg, ok := cmsg.(*types.SignedMessage); ok {
					//		if smsg.Signature.Type == crypto.SigTypeDelegated {
					//			exist = true
					//			break
					//		}
					//	}
					//}

					if exist {
						_, eres, err := components.Stm.ExecutionTraceForEvents(ctx, ts)
						if err != nil {
							alog.Error(err)
							return err
						}

						alog.Infof("execute tipset %v, len(eres): %v, elapsed: %v, eres: %+v", ts.Height(), len(eres), time.Now().Sub(starttime), eres)

						starttime2 := time.Now()
						var eventsRes []*model.EventsRoot
						for _, e := range eres {
							eventsRoot := e.Root
							if eventsRoot != nil {
								events := e.Events
								if len(events) == 0 {
									alog.Errorf("invalid events for root %v", eventsRoot)
									panic(err)
								}

								etm, err := model.NewEventsRoot(*eventsRoot, events, ts.Height())
								if err != nil {
									alog.Errorw("convert to model.EventsRoot", "eventsRoot", eventsRoot, "mcid", e.MCid.String(), "err", err.Error())
								} else {
									eventsRes = append(eventsRes, etm)
								}
							}
						}

						// insert
						var docs []interface{}
						for _, e := range eventsRes {
							docs = append(docs, e)
						}

						total := len(docs)
						if total > 0 {
							ires, err := eventsRootCol.InsertMany(context.TODO(), docs, options.InsertMany().SetOrdered(false))
							if err != nil {
								if actualErr := ExtractActualMgoErrors(err); actualErr != nil {
									alog.Error(err)
									return actualErr
								}
							}

							alog.Infof("ts %v inserted: %v/%v, elapsed: %v\n", ts.Height(), len(ires.InsertedIDs), total, time.Now().Sub(starttime2).String())
						} else {
							alog.Infof("skip tipset %v for no events", ts.Height())
						}
					} else {
						alog.Infof("skip tipset %v for no SigTypeDelegated", ts.Height())
					}

					lk.Lock()
					doneCount++
					alog.Infof("handle tipset %v successfully, elapsed: %v, %v/%v", ts.Height(), time.Now().Sub(start).String(), doneCount, len(tss))
					lk.Unlock()
					return nil
				})
			}

			if err := ewg.Wait(); err != nil {
				return fmt.Errorf("extract part: %w", err)
			}

			alog.Infof("[%v, %v] part done, elapsed: %v", part[0].Height(), part[len(part)-1].Height(), time.Now().Sub(starttime).String())
		}

		log.Infof("replay done")

		return nil
	},
}

var actorEventCmd = &cli.Command{
	Name:  "actor-event",
	Usage: "replay chain data to repo",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "start-height",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "end-ts",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "name",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "chain-storage-repo",
			Usage: "repo path of chain storage",
		},
		&cli.Int64SliceFlag{
			Name: "skip-heights",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := context.Background()

		skipHeights := cctx.Int64Slice("skip-heights")
		log.Infof("skipHeights: %v", skipHeights)

		var components struct {
			fx.In
			CS  *store.ChainStore
			Stm *stmgr.StateManager
		}

		stopper, err := dix.New(ctx,
			Inject(cctx, &components),
		)

		defer stopper(ctx) //nolint:errcheck

		if err != nil {
			return err
		}

		components.CS.StoreEvents(true)
		log.Infof("IsStoringEvents: %v", components.Stm.ChainStore().IsStoringEvents())

		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(cctx.String("dsn")))
		if err != nil {
			return err
		}

		db := client.Database(cctx.String("name"))
		actorEventCol := db.Collection("ActorEvent")

		cids, err := lcli.ParseTipSetString(cctx.String("end-ts"))
		if err != nil {
			return err
		}

		ts, err := components.CS.LoadTipSet(ctx, types.NewTipSetKey(cids...))
		if err != nil {
			return fmt.Errorf("importing chain failed: %w", err)
		}

		start := cctx.Int("start-height")

		log.Infof("start: %v, end: %v", start, ts.Height())

		tss := []*types.TipSet{}

		for ts.Height() >= abi.ChainEpoch(start) {
			tss = append(tss, ts)
			ts, err = components.CS.LoadTipSet(ctx, ts.Parents())
			if err != nil {
				return fmt.Errorf("load ts failed: %w", err)
			}
		}

		got := len(tss)
		for i := 0; i < got/2; i++ {
			j := got - i - 1
			tss[i], tss[j] = tss[j], tss[i]
		}

		var (
			parts  [][]*types.TipSet
			size   = len(tss)
			tsDone = 0
			gap    = 120
		)

		for tsDone < size {
			start := tsDone
			end := start + gap
			if end > size {
				end = size
			}

			part := tss[start:end]
			parts = append(parts, part)
			tsDone += len(part)
		}

		log.Infof("len(tss): %v", size)

		lim := limiter.New(16)
		var ewg multierror.Group

		var (
			doneCount = 0
			lk        sync.Mutex
		)

		for _, part := range parts {
			part := part
			if len(part) == 0 {
				continue
			}

			alog := log.With("range", fmt.Sprintf("[%v, %v]", part[0].Height(), part[len(part)-1].Height()))

			starttime := time.Now()
			alog.Infof("[%v, %v] part begin", part[0].Height(), part[len(part)-1].Height())

			for _, ts := range part {
				ts := ts

				ewg.Go(func() error {
					if !lim.Acquire(context.TODO()) {
						return nil
					}

					defer func() {
						lim.Release(context.TODO())
					}()

					alog.Infof("begin tipset %v", ts.Height())
					start := time.Now()
					skip := false
					for _, h := range skipHeights {
						if h == int64(ts.Height()) {
							skip = true
							break
						}
					}

					if skip {
						alog.Infof("skip tipset %v for existing in skipHeights", ts.Height())

						lk.Lock()
						doneCount++
						alog.Infof("handle tipset %v successfully, %v/%v", ts.Height(), doneCount, len(tss))
						lk.Unlock()

						return nil
					}

					starttime := time.Now()
					cmsgs, err := components.CS.MessagesForTipset(ctx, ts)
					if err != nil {
						alog.Error(err)
						return err
					}

					alog.Infof("get messages for tipset %v, elapsed: %v", ts.Height(), time.Now().Sub(starttime).String())

					starttime = time.Now()
					var exist = true
					//for _, cmsg := range cmsgs {
					//	if smsg, ok := cmsg.(*types.SignedMessage); ok {
					//		if smsg.Signature.Type == crypto.SigTypeDelegated {
					//			exist = true
					//			break
					//		}
					//	}
					//}

					if exist {
						_, eres, err := components.Stm.ExecutionTraceForEvents(ctx, ts)
						if err != nil {
							alog.Error(err)
							return err
						}

						alog.Infof("execute tipset %v, len(eres): %v, elapsed: %v, eres: %+v", ts.Height(), len(eres), time.Now().Sub(starttime), eres)

						starttime2 := time.Now()
						var actorEventsRes []*model.ActorEvent
						for _, e := range eres {
							eventsRoot := e.Root
							if eventsRoot != nil {
								events := e.Events
								if len(events) == 0 {
									alog.Errorf("invalid events for root %v", eventsRoot)
									panic(err)
								}

								seq := make([]int, 0)
								for i, cmsg := range cmsgs {
									if cmsg.Cid().Equals(e.SignedCid) {
										seq = []int{i}
									}
								}

								for i, evt := range events {
									actorID, err := address.NewIDAddress(uint64(evt.Emitter))
									if err != nil {
										return fmt.Errorf("failed to create ID address: %w", err)
									}

									data, topics, ok := ethLogFromEvent(evt.Entries)
									if !ok {
										// not an eth event.
										log.Warnw("ethLogFromEvent not an eth event", "actorID", actorID, "mcid", e.MCid, "signedCid", e.SignedCid)
										//continue //todo
									}

									logIndex := uint64(i)
									removed := false
									aet, err := model.NewActorEvent(actorID, ts.Height(), e.MCid, e.SignedCid, topics, data, logIndex, removed, seq)
									if err != nil {
										log.Errorw("convert to model.ActorEvent", "actorID", actorID, "mcid", e.MCid, "signedCid", e.SignedCid, "err", err.Error())
									} else {
										actorEventsRes = append(actorEventsRes, aet)
									}
								}
							}
						}

						// insert
						var docs []interface{}
						for _, e := range actorEventsRes {
							docs = append(docs, e)
						}

						total := len(docs)
						if total > 0 {
							ires, err := actorEventCol.InsertMany(context.TODO(), docs, options.InsertMany().SetOrdered(false))
							if err != nil {
								if actualErr := ExtractActualMgoErrors(err); actualErr != nil {
									alog.Error(err)
									return actualErr
								}
							}

							alog.Infof("ts %v inserted: %v/%v, elapsed: %v\n", ts.Height(), len(ires.InsertedIDs), total, time.Now().Sub(starttime2).String())
						} else {
							alog.Infof("skip tipset %v for no events", ts.Height())
						}
					} else {
						alog.Infof("skip tipset %v for no SigTypeDelegated", ts.Height())
					}

					lk.Lock()
					doneCount++
					alog.Infof("handle tipset %v successfully, elapsed: %v, %v/%v", ts.Height(), time.Now().Sub(start).String(), doneCount, len(tss))
					lk.Unlock()
					return nil
				})
			}

			if err := ewg.Wait(); err != nil {
				return fmt.Errorf("extract part: %w", err)
			}

			alog.Infof("[%v, %v] part done, elapsed: %v", part[0].Height(), part[len(part)-1].Height(), time.Now().Sub(starttime).String())
		}

		log.Infof("replay done")

		return nil
	},
}

func Inject(cctx *cli.Context, target ...interface{}) dix.Option {
	return dix.Options(
		dix.If(len(target) > 0, dix.Populate(5, target...)),
		dep.ContextModule(context.Background()),
		dep.StateManager(),
		dep.InjectChainRepo(cctx), dep.OfflineDataSource(),
		//dep.InjectFullNode(cctx), dep.OnlineDataSource(),
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

func ExtractActualMgoErrors(err error) error {
	mbwr, ok := err.(mongo.BulkWriteException)
	if !ok {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}

		return err
	}

	var merr error
	for _, we := range mbwr.WriteErrors {
		// from mongo.IsDuplicateKeyError
		if we.Code == 11000 || we.Code == 11001 || we.Code == 12582 {
			continue
		}

		if we.Code == 16460 && strings.Contains(we.Message, " E11000 ") {
			continue
		}

		merr = multierror.Append(merr, err)
	}

	return merr
}

func ethLogFromEvent(entries []types.EventEntry) (data []byte, topics []ethtypes.EthHash, ok bool) {
	var (
		topicsFound      [4]bool
		topicsFoundCount int
		dataFound        bool
	)
	for _, entry := range entries {
		// Drop events with non-raw topics to avoid mistakes.
		if entry.Codec != cid.Raw {
			log.Warnw("did not expect an event entry with a non-raw codec", "codec", entry.Codec, "key", entry.Key)
			return nil, nil, false
		}
		// Check if the key is t1..t4
		if len(entry.Key) == 2 && "t1" <= entry.Key && entry.Key <= "t4" {
			// '1' - '1' == 0, etc.
			idx := int(entry.Key[1] - '1')

			// Drop events with mis-sized topics.
			if len(entry.Value) != 32 {
				log.Warnw("got an EVM event topic with an invalid size", "key", entry.Key, "size", len(entry.Value))
				return nil, nil, false
			}

			// Drop events with duplicate topics.
			if topicsFound[idx] {
				log.Warnw("got a duplicate EVM event topic", "key", entry.Key)
				return nil, nil, false
			}
			topicsFound[idx] = true
			topicsFoundCount++

			// Extend the topics array
			for len(topics) <= idx {
				topics = append(topics, ethtypes.EthHash{})
			}
			copy(topics[idx][:], entry.Value)
		} else if entry.Key == "d" {
			// Drop events with duplicate data fields.
			if dataFound {
				log.Warnw("got duplicate EVM event data")
				return nil, nil, false
			}

			dataFound = true
			data = entry.Value
		} else {
			// Skip entries we don't understand (makes it easier to extend things).
			// But we warn for now because we don't expect them.
			log.Warnw("unexpected event entry", "key", entry.Key)
		}

	}

	// Drop events with skipped topics.
	if len(topics) != topicsFoundCount {
		log.Warnw("EVM event topic length mismatch", "expected", len(topics), "actual", topicsFoundCount)
		return nil, nil, false
	}
	return data, topics, true
}
