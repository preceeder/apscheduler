//   File Name:  main_test.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/26 17:54
//    Change Activity:

package test

import (
	"context"
	"github.com/preceeder/apscheduler/job"
	"log/slog"
)

func test(ctx context.Context, j job.Job) any {
	slog.Info("run job", "jobName", j.Name, "job_id", j.Id, "args", j.Args)
	//time.Sleep(time.Second * 9)
	return "haahh"
}
