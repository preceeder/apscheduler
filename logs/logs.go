//   File Name:  logs.go
//    Description: 日志, 可以根据不同的日志等级写入不同的文件
//    Author:      Chenghu
//    Date:       2024/1/11 17:52
//    Change Activity:

package logs

import (
	"context"
	"log/slog"
)

type ApsLog interface {
	Info(ctx context.Context, msg string, data ...any)
	Error(ctx context.Context, msg string, data ...any)
	Debug(ctx context.Context, msg string, data ...any)
	Warn(ctx context.Context, msg string, data ...any)
}

type DefaultApsLog struct {
}

func (l DefaultApsLog) Info(ctx context.Context, msg string, data ...any) {
	slog.InfoContext(ctx, msg, data...)
}

func (l DefaultApsLog) Error(ctx context.Context, msg string, data ...any) {
	slog.ErrorContext(ctx, msg, data...)
}

func (l DefaultApsLog) Debug(ctx context.Context, msg string, data ...any) {
	slog.DebugContext(ctx, msg, data...)
}
func (l DefaultApsLog) Warn(ctx context.Context, msg string, data ...any) {
	slog.WarnContext(ctx, msg, data...)

}

var DefaultLog ApsLog = DefaultApsLog{}

// SetDefaultLog  设置日志
func SetDefaultLog(l ApsLog) {
	DefaultLog = l
}
