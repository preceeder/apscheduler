//   File Name:  main_test.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/26 17:54
//    Change Activity:

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/preceeder/apscheduler/job"
	"log/slog"
	"testing"
)

func TestNiusd(t *testing.T) {

	//marshal, _ := json.Marshal(sd)

	//fmt.Println(string(marshal))

}

func test(ctx context.Context, j job.Job) any {
	slog.Info("run job", "jobName", j.Name, "job_id", j.Id, "args", j.Args)
	//time.Sleep(time.Second * 9)

	var sd = map[string]any{
		"iud":  12,
		"oid":  []int{1, 3, 4},
		"oids": "sdf",
	}
	marshal, err := json.Marshal(sd)
	if err != nil {
		return nil
	}
	fmt.Println(string(marshal))
	return nil

}
