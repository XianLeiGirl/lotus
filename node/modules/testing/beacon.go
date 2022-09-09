package testing

import (
	"time"

	"github.com/xianleigirl/lotus/build"
	"github.com/xianleigirl/lotus/chain/beacon"
)

func RandomBeacon() (beacon.Schedule, error) {
	return beacon.Schedule{
		{Start: 0,
			Beacon: beacon.NewMockBeacon(time.Duration(build.BlockDelaySecs) * time.Second),
		}}, nil
}
