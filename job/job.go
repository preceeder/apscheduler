//   File Name:  job.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 17:12
//    Change Activity:

package job

import (
	"context"
	"encoding/json"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/triggers"
	"reflect"
	"runtime"
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
	Name    string `json:"name"`
	Trigger triggers.Trigger
	// The job actually runs the function,
	// and you need to register it through 'RegisterFuncs' before using it.
	// Since it cannot be stored by serialization,
	// when using gRPC or HTTP calls, you should use `FuncName`.
	Func func(context.Context, Job) `json:"-"`
	// The actual path of `Func`.
	// This field has a higher priority than `Func`
	FuncName string `json:"func_name"`
	// Arguments for `Func`.
	Args map[string]any `json:"args"`
	// The running timeout of `Func`.
	// Default: `1h`
	Timeout string `json:"timeout"`
	// Automatic update, not manual setting.
	// When the job is paused, this field is set to `9999-09-09 09:09:09`.
	NextRunTime time.Time `json:"next_run_time"`
	// Optional: `STATUS_RUNNING` | `STATUS_PAUSED`
	// It should not be set manually.
	Status string `json:"status"`
	// jobStoreName
	StoreName  string        `json:"store_name"`  // 一旦设置,不能修改
	Replace    bool          `json:"replace"`     // 任务存在是否更新 默认false
	ExpireTime time.Duration `json:"expire_time"` // 任务超过多长时间就是过期, 过期则本次不执行 单位 time.Second
}

// `sort.Interface`, sorted by 'NextRunTime', ascend.
type JobSlice []Job

func (js JobSlice) Len() int           { return len(js) }
func (js JobSlice) Less(i, j int) bool { return js[i].NextRunTime.Before(js[j].NextRunTime) }
func (js JobSlice) Swap(i, j int)      { js[i], js[j] = js[j], js[i] }

// Initialization functions for each job,
// called when the scheduler run `AddJob`.
func (j *Job) Init() error {
	j.Status = STATUS_RUNNING

	if j.FuncName == "" {
		j.FuncName = getFuncName(j.Func)
	}

	if j.Timeout == "" {
		j.Timeout = "1h"
	}

	if j.ExpireTime == 0 {
		j.ExpireTime = time.Second * 1
	}

	err := j.Trigger.Init()
	if err != nil {
		return err
	}

	nextRunTime, err := j.Trigger.GetNextRunTime(time.Time{}, time.Now())
	if err != nil {
		return err
	}
	j.NextRunTime = nextRunTime

	if err := j.Check(); err != nil {
		return err
	}

	return nil
}

// Called when the job run `init` or scheduler run `UpdateJob`.
func (j *Job) Check() error {
	if _, ok := FuncMap[j.FuncName]; !ok {
		return apsError.FuncUnregisteredError(j.FuncName)
	}

	_, err := time.ParseDuration(j.Timeout)
	if err != nil {
		return &apsError.JobTimeoutError{FullName: j.Name, Timeout: j.Timeout, Err: err}
	}

	return nil
}

// NextRunTimeHandler 下次执行时间处理, 知道处理为离now时间最短的下一次
func (j *Job) NextRunTimeHandler(now time.Time) (time.Time, bool, error) {
	nextRunTIme := j.NextRunTime

	var err error
	var IsExpire bool
	if now.Sub(nextRunTIme) >= j.ExpireTime {
		// 本次任务过期, 不执行
		IsExpire = true
	}

	for !nextRunTIme.IsZero() && nextRunTIme.Before(now) {
		nextRunTIme, err = j.Trigger.GetNextRunTime(nextRunTIme, now)
		if err != nil {
			break
		}
	}
	return nextRunTIme, IsExpire, err

}

func (j Job) String() string {

	jobStr, _ := json.Marshal(j)
	return string(jobStr)
}

// StateDump Serialize Job and convert to Bytes

type FuncPkg struct {
	Func func(context.Context, Job)
	// About this function.
	Info string
}

var FuncMap = make(map[string]FuncPkg)

func FuncMapReadable() []map[string]string {
	funcs := []map[string]string{}
	for fName, fPkg := range FuncMap {
		funcs = append(funcs, map[string]string{
			"name": fName, "info": fPkg.Info,
		})
	}

	return funcs
}

func RegisterFuncs(fps ...FuncPkg) {
	for _, fp := range fps {
		fName := getFuncName(fp.Func)
		FuncMap[fName] = fp
	}
}

func getFuncName(f func(context.Context, Job)) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}
