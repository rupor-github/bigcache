package bigcache

import "time"

type clock interface {
	epoch() uint64
}

type systemClock struct{}

func (c systemClock) epoch() uint64 {
	return uint64(time.Now().Unix())
}
