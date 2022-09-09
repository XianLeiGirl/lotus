package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"

	"github.com/xianleigirl/lotus/api"
	lcli "github.com/xianleigirl/lotus/cli"
	"github.com/xianleigirl/lotus/lib/tablewriter"
)

var dagstoreCmd = &cli.Command{
	Name:  "dagstore",
	Usage: "Manage the dagstore on the markets subsystem",
	Subcommands: []*cli.Command{
		dagstoreListShardsCmd,
		dagstoreRegisterShardCmd,
		dagstoreInitializeShardCmd,
		dagstoreRecoverShardCmd,
		dagstoreInitializeAllCmd,
		dagstoreGcCmd,
		dagstoreLookupPiecesCmd,
	},
}

var dagstoreListShardsCmd = &cli.Command{
	Name:  "list-shards",
	Usage: "List all shards known to the dagstore, with their current status",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shards, err := marketsApi.DagstoreListShards(ctx)
		if err != nil {
			return err
		}

		return printTableShards(shards)
	},
}

var dagstoreRegisterShardCmd = &cli.Command{
	Name:      "register-shard",
	ArgsUsage: "[key]",
	Usage:     "Register a shard",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		marketsAPI, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shardKey := cctx.Args().First()
		err = marketsAPI.DagstoreRegisterShard(ctx, shardKey)
		if err != nil {
			return err
		}

		fmt.Println("Registered shard " + shardKey)
		return nil
	},
}

var dagstoreInitializeShardCmd = &cli.Command{
	Name:      "initialize-shard",
	ArgsUsage: "[key]",
	Usage:     "Initialize the specified shard",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return marketsApi.DagstoreInitializeShard(ctx, cctx.Args().First())
	},
}

var dagstoreRecoverShardCmd = &cli.Command{
	Name:      "recover-shard",
	ArgsUsage: "[key]",
	Usage:     "Attempt to recover a shard in errored state",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return marketsApi.DagstoreRecoverShard(ctx, cctx.Args().First())
	},
}

var dagstoreInitializeAllCmd = &cli.Command{
	Name:  "initialize-all",
	Usage: "Initialize all uninitialized shards, streaming results as they're produced; only shards for unsealed pieces are initialized by default",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:     "concurrency",
			Usage:    "maximum shards to initialize concurrently at a time; use 0 for unlimited",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "include-sealed",
			Usage: "initialize sealed pieces as well",
		},
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		concurrency := cctx.Uint("concurrency")
		sealed := cctx.Bool("sealed")

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		params := api.DagstoreInitializeAllParams{
			MaxConcurrency: int(concurrency),
			IncludeSealed:  sealed,
		}

		ch, err := marketsApi.DagstoreInitializeAll(ctx, params)
		if err != nil {
			return err
		}

		for {
			select {
			case evt, ok := <-ch:
				if !ok {
					return nil
				}
				_, _ = fmt.Fprint(os.Stdout, color.New(color.BgHiBlack).Sprintf("(%d/%d)", evt.Current, evt.Total))
				_, _ = fmt.Fprint(os.Stdout, " ")
				if evt.Event == "start" {
					_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.Reset).Sprint("STARTING"))
				} else {
					if evt.Success {
						_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.FgGreen).Sprint("SUCCESS"))
					} else {
						_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.FgRed).Sprint("ERROR"), evt.Error)
					}
				}

			case <-ctx.Done():
				return fmt.Errorf("aborted")
			}
		}
	},
}

var dagstoreGcCmd = &cli.Command{
	Name:  "gc",
	Usage: "Garbage collect the dagstore",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		collected, err := marketsApi.DagstoreGC(ctx)
		if err != nil {
			return err
		}

		if len(collected) == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "no shards collected")
			return nil
		}

		for _, e := range collected {
			if e.Error == "" {
				_, _ = fmt.Fprintln(os.Stdout, e.Key, color.New(color.FgGreen).Sprint("SUCCESS"))
			} else {
				_, _ = fmt.Fprintln(os.Stdout, e.Key, color.New(color.FgRed).Sprint("ERROR"), e.Error)
			}
		}

		return nil
	},
}

func printTableShards(shards []api.DagstoreShardInfo) error {
	if len(shards) == 0 {
		return nil
	}

	tw := tablewriter.New(
		tablewriter.Col("Key"),
		tablewriter.Col("State"),
		tablewriter.Col("Error"),
	)

	colors := map[string]color.Attribute{
		"ShardStateAvailable": color.FgGreen,
		"ShardStateServing":   color.FgBlue,
		"ShardStateErrored":   color.FgRed,
		"ShardStateNew":       color.FgYellow,
	}

	for _, s := range shards {
		m := map[string]interface{}{
			"Key": s.Key,
			"State": func() string {
				trimmedState := strings.TrimPrefix(s.State, "ShardState")
				if c, ok := colors[s.State]; ok {
					return color.New(c).Sprint(trimmedState)
				}
				return trimmedState
			}(),
			"Error": s.Error,
		}
		tw.Write(m)
	}
	return tw.Flush(os.Stdout)
}

var dagstoreLookupPiecesCmd = &cli.Command{
	Name:      "lookup-pieces",
	Usage:     "Lookup pieces that a given CID belongs to",
	ArgsUsage: "<cid>",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a CID")
		}

		cidStr := cctx.Args().First()
		cid, err := cid.Parse(cidStr)
		if err != nil {
			return fmt.Errorf("invalid CID: %w", err)
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shards, err := marketsApi.DagstoreLookupPieces(ctx, cid)
		if err != nil {
			return err
		}

		return printTableShards(shards)
	},
}
