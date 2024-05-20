### 使用

```go
package main


import (
	"context"
	"fmt"
	"github.com/preceeder/apscheduler"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"log/slog"
	"time"
)

func test(ctx context.Context, j job.Job) {
	slog.Info("run job", "jobName", j.Name)
}

func main(){
	// 注册任务函数
	job.RegisterJobsFunc(job.FuncInfo{Func: test, Name: "printJob", Description: "测试使用"})
    
	// 注册 监听事件
	events.RegisterEvent(events.EVENT_JOB_ERROR|events.EVENT_JOB_ADDED|events.EVENT_JOB_REMOVED, func(ei events.EventInfo) {
		fmt.Println("lisenter:--->", ei)
	})
	
	// 初始化 Scheduelr 
	scheduler := apscheduler.NewScheduler()
    
	// 注册redis 存储器
	redis := stores.RedisConfig{
		Host:        "127.0.0.1",
		Port:        "6379",
		Password:    "",
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
	
	// 注册mysql 存储器
	mysql := stores.MysqlConfig{
		Host:        "127.0.0.1",
		Port:        "3306",
		Password:    "job",
		User:        "job",
		Db:          "job",
		MaxOpenCons: 20,
		MaxIdleCons: 5,
	}
	store := stores.NewMysqlStore(mysql, "")
	err = scheduler.SetStore("mysql", store)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	
	// 添加任务1
	job1 := job.Job{
		Name:     "job1",
		Id:       "job1",  // 全局唯一id
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			Interval: 5 * 1000,  // 3s
			TimeZoneName: "UTC+8",
			EndTime: "2024-05-20 16:13:26",
		},

		Replace:   true,
		StoreName: "redis",
		Args:      map[string]any{"arg1": "1", "arg2": "2", "arg3": "3"},
	}
	job1, err = scheduler.AddJob(job1)
	if err != nil {
		fmt.Println(err)
	}
    
	
	// 开始执行,   开始后还可以继续添加任务
	scheduler.Start()

	job2 := job.Job{
		Name:     "job2",
		Id:       "job2",
		FuncName: "printJob",
		Trigger: &triggers.IntervalTrigger{
			Interval: 3 * 1000,  // 3s
		},
		Replace:   true,
		StoreName: "mysql",
		Args:      map[string]any{"arg1": "mysql", "arg2": "2", "arg3": "3"},
	}
	job2, err = scheduler.AddJob(job2)
	if err != nil {
		fmt.Println(err)
	}
	
	select {}


}

```