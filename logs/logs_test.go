//   File Name:  logs_test.go.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/3/20 10:18
//    Change Activity:

package logs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkTestNewSlog(b *testing.B) {
	cfg := SlogConfig{
		ErrorFileName:           "logs/error.log",
		InfoFileName:            "logs/out.log",
		TransparentTransmission: true,
		StdOut:                  "0",
		MaxSize:                 1,
		MaxAge:                  3,
		MaxBackups:              4,
	}
	NewSlog(cfg)
	//i := 1000
	//j := 0
	slog.InfoContext(context.Background(), "sds")

	//for j <= b.N {
	//	j++
	//	slog.InfoContext(ctx, "sds")
	//	slog.ErrorContext(ctx, "hahah", "error", "ssss")
	//	//slog.Info("", "sss", j)
	//}

}

func BenchmarkNewSlog(b *testing.B) {
	file := "scabhvuiehviuusdvuauisgvuiav"
	line := 3
	//j := 0
	//for j <= b.N {
	//	j++
	//	//fmt.Sprintf("%s:%d", file, line)
	//	//
	//	_ = strings.Join([]string{file, strconv.Itoa(line)}, ":")
	//
	//}

	dd := strings.Join([]string{file, strconv.Itoa(line)}, ":")
	fmt.Println(dd)
}
