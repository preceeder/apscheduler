//   File Name:  main_test.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/26 17:54
//    Change Activity:

package apscheduler

import (
	"context"
	"fmt"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"log/slog"
	"testing"
	"time"
)

func TestRunMain(t *testing.T) {
	tests := []struct {
		name string
		want *Scheduler
	}{
		// TODO: Add test cases.
		{name: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunMain()
		})
	}
}

func test(ctx context.Context, j job.Job) {
	slog.Info("run job", "jobName", j.Name)
	//time.Sleep(time.Second * 9)
}

func RunMain() {
	scheduler := NewScheduler()
	events.RegisterEvent(events.EVENT_JOB_ERROR|events.EVENT_JOB_ADDED|events.EVENT_JOB_REMOVED, func(ei events.EventInfo) {
		fmt.Println("lisenter:--->", ei)
	})

	job.RegisterJobsFunc(job.FuncInfo{Func: test, Name: "printJob", Description: "测试使用"})

	redis := stores.RedisConfig{
		Host: "127.0.0.1",
		//Port: "6379",
		Port:        "56379",
		Password:    "QDjk9UdkoD6cv",
		Db:          0,
		MaxIdle:     2,
		IdleTimeout: 240,
		PoolSize:    10,
	}
	store1 := stores.NewRedisStore(redis, "", "")
	err := scheduler.SetStore("redis", store1)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	startTime := time.Now().Format(time.DateTime)
	job1 := job.Job{
		Name:     "job1",
		Id:       "job1",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    startTime,
			Interval:     3 * 1000,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "redis",
		Args:      map[string]any{"arg1": "1", "arg2": "2", "arg3": "3"},
	}

	job1, err = scheduler.AddJob(job1)
	if err != nil {
		fmt.Println(err)
	}
	//
	//mysql := stores.MysqlConfig{
	//	Host:        "127.0.0.1",
	//	Port:        "13306",
	//	Password:    "9fdIWuOJODW55sUG",
	//	User:        "matchus",
	//	Db:          "match_dev01",
	//	MaxOpenCons: 20,
	//	MaxIdleCons: 5,
	//}
	//store := stores.NewMysqlStore(mysql, "")
	//err := scheduler.SetStore("mysql", store)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//
	job2 := job.Job{
		Name:     "job3",
		Id:       "job3",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    startTime,
			Interval:     3 * 1000,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "redis",
		Args:      map[string]any{"arg1": "mysql", "arg2": "2", "arg3": "3"},
	}
	job2, err = scheduler.AddJob(job2)
	if err != nil {
		fmt.Println(err)
	}

	job4 := job.Job{
		Name:     "job4",
		Id:       "job4",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    startTime,
			Interval:     3 * 1000,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "redis",
		Args:      map[string]any{"arg1": "mysql", "arg2": "2", "arg3": "3"},
	}
	job4, err = scheduler.AddJob(job4)
	if err != nil {
		fmt.Println(err)
	}

	job5 := job.Job{
		Name:     "job5",
		Id:       "job5",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    startTime,
			Interval:     3 * 1000,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "redis",
		Args:      map[string]any{"arg1": "mysql", "arg2": "2", "arg3": "3"},
	}
	job5, err = scheduler.AddJob(job5)
	if err != nil {
		fmt.Println(err)
	}

	scheduler.Start()

	select {}
}
