package apsContext

import (
	"context"
	"github.com/preceeder/apscheduler/common"
)

type Context struct {
	context.Context
	RequestId string
}

func NewContext() Context {
	return Context{
		context.Background(),
		common.RandStr(16),
	}
}
