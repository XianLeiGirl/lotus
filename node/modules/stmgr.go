package modules

import (
	"go.uber.org/fx"

	"github.com/xianleigirl/lotus/chain/beacon"
	"github.com/xianleigirl/lotus/chain/stmgr"
	"github.com/xianleigirl/lotus/chain/store"
	"github.com/xianleigirl/lotus/chain/vm"
)

func StateManager(lc fx.Lifecycle, cs *store.ChainStore, exec stmgr.Executor, sys vm.SyscallBuilder, us stmgr.UpgradeSchedule, b beacon.Schedule) (*stmgr.StateManager, error) {
	sm, err := stmgr.NewStateManager(cs, exec, sys, us, b)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: sm.Start,
		OnStop:  sm.Stop,
	})
	return sm, nil
}
