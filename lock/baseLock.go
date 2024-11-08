package lock

import (
	"context"
)

type Lock interface {
	GetLock(context.Context, string) (bool, error, string)
	ReleaseLock(context.Context, string, string) error
}
