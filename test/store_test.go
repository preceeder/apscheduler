//   File Name:  store_test.go
//    Description: 使用多个store 处理job
//    Author:      Chenghu
//    Date:       2024/6/13 17:57
//    Change Activity:

package test

import (
	"fmt"
	"github.com/preceeder/apscheduler"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"testing"
)

func TestRunMoreStore(t *testing.T) {
	tests := []struct {
		name string
		want *apscheduler.Scheduler
	}{
		// TODO: Add test cases.
		{name: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunStore()
		})
	}
}

func RunStore() {
	scheduler := apscheduler.NewScheduler()
	events.RegisterEvent(events.EVENT_JOB_ERROR|events.EVENT_JOB_ADDED|events.EVENT_JOB_REMOVED|events.EVENT_JOB_EXECUTED, func(ei events.EventInfo) {
		fmt.Println("lisenter:--->", ei)
	})

	job.RegisterJobsFunc(job.FuncInfo{Func: test, Name: "printJob", Description: "测试使用"})

	// redis store
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

	// memory store
	def := stores.MemoryStore{}
	err = scheduler.SetStore("default", &def)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// mysql store
	mysql := stores.MysqlConfig{
		Host:        "127.0.0.1",
		Port:        "13306",
		Password:    "9fdIWuOJODW55sUG",
		User:        "matchus",
		Db:          "match_dev01",
		MaxOpenCons: 20,
		MaxIdleCons: 5,
	}
	store := stores.NewMysqlStore(mysql, "")
	err = scheduler.SetStore("mysql", store)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	//startTime := time.Now().Format(time.DateTime)
	job1 := job.Job{
		Name:     "job01",
		Id:       "test002",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    "",
			Interval:     5,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "default",
		Args:      map[string]any{"arg1": "1", "arg2": "2", "arg3": "3"},
	}

	job1, err = scheduler.AddJob(job1)
	if err != nil {
		fmt.Println(err)
	}
	//

	//
	job2 := job.Job{
		Name:     "job3",
		Id:       "job3",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    "",
			Interval:     3,
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
	//
	job4 := job.Job{
		Name:     "job4",
		Id:       "job4",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    "",
			Interval:     3,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "mysql",
		Args:      map[string]any{"arg1": "mysql", "arg2": "2", "arg3": "3"},
	}
	job4, err = scheduler.AddJob(job4)
	if err != nil {
		fmt.Println(err)
	}

	scheduler.Start()

	select {}
}
