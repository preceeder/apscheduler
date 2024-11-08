//   File Name:  logs_test.go.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/3/20 10:18
//    Change Activity:

package logs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkTestNewSlog(b *testing.B) {

	DefaultLog.Info(context.Background(), "sds")

	//for j <= b.N {
	//	j++
	//		DefaultLog.Info(apsContext.Background(), "sds")
	//		DefaultLog.Info(apsContext.Background(), "sds")
	//	//	DefaultLog.Info(apsContext.Background(), "sds")
	//}

}

func BenchmarkNewSlog(b *testing.B) {
	file := "scabhvuiehviuusdvuauisgvuiav"
	line := 3

	dd := strings.Join([]string{file, strconv.Itoa(line)}, ":")
	fmt.Println(dd)
}
