//   File Name:  distributed_test.go
//    Description: 分布式锁处理
//    Author:      Chenghu
//    Date:       2024/6/13 18:01
//    Change Activity:

package test

import (
	"context"
	"fmt"
	"github.com/preceeder/apscheduler"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

type RedisConfig struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Password    string `json:"password"`
	Db          int    `json:"db"`
	MaxIdle     int    `json:"maxIdle"`
	IdleTimeout int    `json:"idleTimeout"`
	PoolSize    int    `json:"PoolSize"`
}

var rdb *redis.Client

func TestRunDistributed(t *testing.T) {
	rc := RedisConfig{
		Host: "127.0.0.1",
		//Port: "6379",
		Port:        "56379",
		Password:    "QDjk9UdkoD6cv",
		Db:          0,
		MaxIdle:     2,
		IdleTimeout: 240,
		PoolSize:    10,
	}
	rdb = getRedis(rc)

	go Distributed01()
	time.Sleep(time.Second * 1)
	go Distributed02()

	select {}
}

func getRedis(cf RedisConfig) *redis.Client {
	addr := cf.Host + ":" + cf.Port
	redisOpt := &redis.Options{
		Addr:         addr,
		Password:     cf.Password,
		DB:           cf.Db,
		PoolSize:     cf.PoolSize,
		MaxIdleConns: cf.MaxIdle,
		MinIdleConns: cf.MaxIdle,
	}
	rdb := redis.NewClient(redisOpt)
	cmd := rdb.Ping(context.Background())
	if cmd.Err() != nil {
		panic("redis connect fail")
	}

	return rdb
}

func Distributed01() {
	scheduler := apscheduler.NewScheduler()
	events.RegisterEvent(events.EVENT_JOB_ERROR|events.EVENT_JOB_ADDED|events.EVENT_JOB_REMOVED|events.EVENT_JOB_EXECUTED, func(ei events.EventInfo) {
		fmt.Println("lisenter:--->", ei)
	})

	job.RegisterJobsFunc(job.FuncInfo{Func: test, Name: "printJob", Description: "测试使用"})

	def := stores.MemoryStore{}
	_ = scheduler.SetStore("default", &def)

	// 设置  启用分布式锁
	scheduler.SetDistributed(rdb, "")

	//startTime := time.Now().Format(time.DateTime)
	job1 := job.Job{
		Name:     "job01",
		Id:       "test002",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    "",
			Interval:     2,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "default",
		Args:      map[string]any{"arg1": "1", "arg2": "2", "arg3": "3"},
	}

	job1, err := scheduler.AddJob(job1)
	if err != nil {
		fmt.Println(err)
	}
	scheduler.Start()
}

func Distributed02() {
	scheduler := apscheduler.NewScheduler()
	events.RegisterEvent(events.EVENT_JOB_ERROR|events.EVENT_JOB_ADDED|events.EVENT_JOB_REMOVED|events.EVENT_JOB_EXECUTED, func(ei events.EventInfo) {
		fmt.Println("lisenter:--->", ei)
	})

	job.RegisterJobsFunc(job.FuncInfo{Func: test, Name: "printJob", Description: "测试使用"})

	def := stores.MemoryStore{}
	_ = scheduler.SetStore("default", &def)

	// 设置  启用分布式锁
	scheduler.SetDistributed(rdb, "")

	job1 := job.Job{
		Name:     "job013333",
		Id:       "test002",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			StartTime:    "",
			Interval:     1,
			TimeZoneName: "UTC+8",
		},
		Replace:   true,
		StoreName: "default",
		Args:      map[string]any{"arg1": "1", "arg2": "2", "arg3": "3"},
	}

	job1, err := scheduler.AddJob(job1)
	if err != nil {
		fmt.Println(err)
	}

	scheduler.Start()
}
