//   File Name:  handler.go
//    Description: 使不同等级的日志, 写入对应的文件中
//    Author:      Chenghu
//    Date:       2024/1/11 11:12
//    Change Activity:

package logs

import (
	"context"
	"io"
	"log/slog"
)

type MoreHandler struct {
	TransparentTransmission bool // 日志是否往高等级传递 LevelErr(8) -> LevelWarn(4) -> LevelInfo(0) -> LevelDebug(-4)
	MHandler                map[slog.Level]*slog.JSONHandler
	MinLevel                slog.Level // 最小的 level    如果有 debug, info 最小的就是 info  	LevelDebug = -4 ,LevelInfo = 0 ,LevelWarn = 4 ,LevelError = 8
}

func NewMoreHandler(w map[slog.Level]io.Writer, minLevel slog.Level, transparentTransmission bool, opts *slog.HandlerOptions) *MoreHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}

	handler := MoreHandler{MinLevel: minLevel, MHandler: map[slog.Level]*slog.JSONHandler{}, TransparentTransmission: transparentTransmission}
	for level, wl := range w {
		handler.MHandler[level] = slog.NewJSONHandler(wl, opts)
	}
	return &handler
}

func (h *MoreHandler) Enabled(_ context.Context, level slog.Level) bool {
	if level >= h.MinLevel {
		return true
	}
	return false
}

func (h *MoreHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &MoreHandler{}
}

func (h *MoreHandler) WithGroup(name string) slog.Handler {
	return &MoreHandler{}
}

func (h *MoreHandler) Handle(c context.Context, r slog.Record) error {
	w := false
	for level, handler := range h.MHandler {
		if h.TransparentTransmission && r.Level >= level {
			w = true
			_ = handler.Handle(c, r)
			continue
		} else if level == r.Level {
			w = true
			_ = handler.Handle(c, r)
			continue
		}
	}
	if !w {
		return h.MHandler[h.MinLevel].Handle(c, r)
	}
	return nil

	//if handler, ok := h.MHandler[r.Level]; ok {
	//	return handler.Handle(c, r)
	//} else {
	//	return h.MHandler[h.MinLevel].Handle(c, r)
	//}
}
