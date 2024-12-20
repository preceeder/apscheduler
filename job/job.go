//   File Name:  job.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 17:12
//    Change Activity:

package job

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/duke-git/lancet/v2/cryptor"
	"github.com/preceeder/apscheduler/apsContext"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/triggers"
	"reflect"
	"time"
)

const (
	STATUS_RUNNING = "running"
	STATUS_PAUSED  = "paused"
)

type Job struct {
	// 任务的唯一id.
	Id string `json:"id"` // 一旦设置,不能修改
	// job name
	Name        string           `json:"name"`
	Trigger     triggers.Trigger `json:"trigger"`
	TriggerName string           `json:"triggerName"` // 留给内部使用的
	// 注册函数名
	FuncName string `json:"func_name"` // 必须和注册的函数名一致
	// Arguments for `Func`.
	Args map[string]any `json:"args"`
	// The running timeout of `Func`.
	// ms Default: 20 s
	Timeout int64 `json:"timeout"`
	// Automatic update, not manual setting.  s
	NextRunTime int64 `json:"next_run_time"`
	// Optional: `STATUS_RUNNING` | `STATUS_PAUSED`
	// It should not be set manually.
	Status string `json:"status"`
	// jobStoreName
	StoreName   string `json:"store_name"`   // 一旦设置,不能修改
	Replace     bool   `json:"replace"`      // 任务存在是否更新 默认false
	MaxInstance int    `json:"max_instance"` // 改任务可以同时存在的个数 最少1个, 默认 1
	HashValue   string `json:"has_value"`    // 保存job本身 除NextRunTime 以外的 数据的has
}

// 转成map 用于创建任务时，请求传输map
func (js Job) ToMap() map[string]any {
	return map[string]any{
		"id":           js.Id,
		"name":         js.Name,
		"trigger":      js.Trigger.ToMap(),
		"func_name":    js.FuncName,
		"args":         js.Args,
		"timeout":      js.Timeout,
		"store_name":   js.StoreName,
		"replace":      js.Replace,
		"max_instance": js.MaxInstance,
	}
}

// `sort.Interface`, sorted by 'NextRunTime', ascend.
type JobSlice []Job

func (js JobSlice) Len() int           { return len(js) }
func (js JobSlice) Less(i, j int) bool { return js[i].NextRunTime < js[j].NextRunTime }
func (js JobSlice) Swap(i, j int)      { js[i], js[j] = js[j], js[i] }

// Initialization functions for each job,
// called when the scheduler run `AddJob`.
func (j *Job) Init() error {
	j.Status = STATUS_RUNNING

	if j.FuncName == "" {
		return apsError.FuncNameNullError(j.Id)
	}

	if j.Timeout == 0 {
		j.Timeout = 20 // 默认20秒
	}

	err := j.Trigger.Init()
	if err != nil {
		return err
	}
	j.TriggerName = reflect.TypeOf(j.Trigger).Elem().Name()
	nextRunTime, err := j.Trigger.GetNextRunTime(0, time.Now().UTC().Unix())
	if err != nil {
		return err
	}
	if nextRunTime == 0 {
		return errors.New("endTime can't lt startTime")
	}

	if j.MaxInstance == 0 {
		j.MaxInstance = 1
	}
	j.Has()

	j.NextRunTime = nextRunTime

	if err := j.Check(); err != nil {
		return err
	}

	return nil
}

func (j *Job) Has() {
	j.NextRunTime = 0
	jstr := j.String()
	j.HashValue = cryptor.Md5String(jstr)
}

func (j *Job) Check() error {
	// 检查任务函数是否存在
	if _, ok := FuncMap[j.FuncName]; !ok {
		return apsError.FuncUnregisteredError(j.FuncName)
	}
	return nil
}

// NextRunTimeHandler 下次执行时间处理, 知道处理为离now时间最短的下一次
func (j *Job) NextRunTimeHandler(ctx apsContext.Context, nowi int64) (int64, bool, error) {
	nextRunTIme := j.NextRunTime

	var err error
	var IsExpire bool
	jitter := j.Trigger.GetJitterTime()
	if jitter > 0 && nowi-nextRunTIme >= jitter {
		// 本次任务过期, 不执行
		IsExpire = true
	}

	//for nextRunTIme != 0 && math.Abs(float64(nextRunTIme-nowi)) < float64(j.Trigger.GetExpireTime()) {
	for nextRunTIme != 0 && nextRunTIme <= nowi {
		nextRunTIme, err = j.Trigger.GetNextRunTime(nextRunTIme, nowi)
		if err != nil {
			logs.DefaultLog.Info(ctx, "NextRunTimeHandler", "job", j, "error", err.Error())
			break
		}
	}
	return nextRunTIme, IsExpire, err

}

func (j Job) String() string {
	jobStr, _ := json.Marshal(j)
	return string(jobStr)
}

type FuncInfo struct {
	Func        func(context.Context, Job) any
	Name        string // 全局唯一函数标志
	Description string // 函数描述
}

var FuncMap = make(map[string]FuncInfo)

// RegisterJobsFunc 注册全局唯一函数
func RegisterJobsFunc(fis ...FuncInfo) {
	for _, fi := range fis {
		FuncMap[fi.Name] = fi
	}
}
