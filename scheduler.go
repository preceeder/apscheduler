//   File Name:  scheduler.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 17:12
//    Change Activity:

package apscheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/preceeder/apscheduler/apsContext"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/lock"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"github.com/preceeder/apscheduler/try"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Instance[T int] struct {
	Instances sync.Map
}

func (i *Instance[T]) Add(key string, num T) {
	if v, ok := i.Instances.Load(key); ok {
		i.Instances.Store(key, v.(T)+num)
	} else {
		i.Instances.Store(key, num)
	}
}

func (i *Instance[T]) Sub(key string, num T) {
	if v, ok := i.Instances.Load(key); ok {
		i.Instances.Store(key, v.(T)-num)
	} else {
		i.Instances.Store(key, -num)
	}
}

func (i *Instance[T]) Get(key string) (res T) {
	if v, ok := i.Instances.Load(key); ok {
		res = v.(T)
	} else {
		return
	}
	return
}

type Scheduler struct {
	// Job store
	store map[string]stores.Store
	// When the time is up, the scheduler will wake up.
	timer *time.Timer
	// reset timer
	jobChangeChan chan struct{}
	instances     Instance[int]
	// It should not be set manually.
	isRunning bool
	mutexS    sync.RWMutex
	// Input is received when `stop` is called
	ctx         context.Context
	cancel      context.CancelFunc
	pool        *ants.Pool // 默认10000,   会被 执行job 和 update job 平分
	distributed lock.Lock  // 启用分布式

}

// NewScheduler 默认创建一个 MemoryStore
func NewScheduler() *Scheduler {
	//var store map[string]stores.Store = map[string]stores.Store{stores.DefaultName: &stores.MemoryStore{}}
	ctx, cancel := context.WithCancel(context.Background())

	events.StartEventsListen(ctx)
	pool, err := ants.NewPool(1000, ants.WithNonblocking(true))
	if err != nil {
		fmt.Println("init pool failed", "error", err.Error())
		os.Exit(1)
	}

	return &Scheduler{
		store:  map[string]stores.Store{},
		mutexS: sync.RWMutex{},
		instances: Instance[int]{
			Instances: sync.Map{},
		},
		distributed: nil,
		ctx:         ctx,
		cancel:      cancel,
		pool:        pool,
	}
}

func (s *Scheduler) IsRunning() bool {
	//s.mutexS.RLock()
	//defer s.mutexS.RUnlock()

	return s.isRunning
}

// Bind the store
func (s *Scheduler) SetStore(name string, sto stores.Store) (err error) {
	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOBSTORE_ADDED,
			Store:     &sto,
			Error:     err,
			Msg:       strings.Join([]string{"store name: ", name}, ""),
		}
	}()
	s.store[name] = sto
	if err = s.store[name].Init(); err != nil {
		return err
	}

	return nil
}

// 配置分布式锁
func (s *Scheduler) SetDistributed(lk lock.Lock) {
	s.distributed = lk
}

// RemoveStore remove store
func (s *Scheduler) RemoveStore(name string) (err error) {
	var Sotre stores.Store

	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOBSTORE_REMOVED,
			Store:     &Sotre,
			Error:     err,
			Msg:       strings.Join([]string{"store name: ", name}, ""),
		}
	}()
	if sto, exists := s.store[name]; exists {
		Sotre = sto
		err := sto.Close()
		delete(s.store, name)
		return err
	}
	return nil
}

func (s *Scheduler) getStore(name ...string) (stores.Store, error) {
	storeName := stores.DefaultName
	if len(name) > 0 {
		storeName = name[0]
	}
	if store, ok := s.store[storeName]; ok {
		return store, nil
	}
	return nil, errors.New(strings.Join([]string{"store name ", storeName, " not find!"}, ""))
}

// GetAllStoreName 获取当前所有的 store name
func (s *Scheduler) GetAllStoreName() []string {
	storeNames := make([]string, 0)
	for k, _ := range s.store {
		storeNames = append(storeNames, k)
	}
	return storeNames
}

func (s *Scheduler) _runJob(ctx apsContext.Context, j job.Job) {
	defer func() {
		if err := recover(); err != nil {
			logs.DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
		}
	}()
	f := reflect.ValueOf(job.FuncMap[j.FuncName].Func)
	if f.IsNil() {
		msg := fmt.Sprintf("Job `%s` Func `%s` unregistered", j.Name, j.FuncName)
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_JOB_ERROR,
			Job:       &j,
			Error:     errors.New(msg),
		}
		logs.DefaultLog.Warn(ctx, msg)
	} else {
		s.instances.Add(j.Id, 1)
		defer func() {
			s.instances.Sub(j.Id, 1)
		}()

		ctt, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(j.Timeout))
		defer cancel()

		ch := make(chan error, 1)
		go func() {
			var result any
			defer close(ch)
			defer func() {
				if err := recover(); err != nil {
					logs.DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` run error: %s", j.Name, try.PrintStackTrace(err)))
					events.EventChan <- events.EventInfo{
						Ctx:       ctx,
						EventCode: events.EVENT_JOB_ERROR,
						Job:       &j,
						Error:     err.(error),
					}
				} else {
					events.EventChan <- events.EventInfo{
						Ctx:       ctx,
						EventCode: events.EVENT_JOB_EXECUTED,
						Job:       &j,
						Result:    result,
					}
				}
			}()

			res := f.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(j)})
			if len(res) > 0 {
				result = res[0].Interface()
			}
		}()

		select {
		case <-ch:
			return
		case <-ctt.Done():
			err := fmt.Sprintf("Job `%s` run timeout", j.Id)
			logs.DefaultLog.Warn(ctx, err)

			events.EventChan <- events.EventInfo{
				Ctx:       ctx,
				EventCode: events.EVENT_JOB_ERROR,
				Job:       &j,
				Error:     errors.New(err),
			}
		}
	}
}

func (s *Scheduler) _flushJob(ctx apsContext.Context, j job.Job, lockValue string) func() {
	return func() {
		//defer wg.Done()
		defer func() {
			if s.distributed != nil {
				err := s.distributed.ReleaseLock(ctx, j.Id, lockValue)
				if err != nil {
					logs.DefaultLog.Error(ctx, "redis lock release failed", "error", err.Error())
				}
			}
		}()
		// 在这里还需要再次检查任务是否有其他并发问题
		if j.NextRunTime == 0 {
			if err := s._deleteJob(ctx, j.Id, j.StoreName); err != nil {
				logs.DefaultLog.Error(ctx, fmt.Errorf("Scheduler delete job `%s` error: %s", j.Id, err).Error())
				return
			}
		} else {
			if _, err := s._updateJob(ctx, j, j.StoreName, false); err != nil {
				logs.DefaultLog.Error(ctx, fmt.Errorf("Scheduler update job `%s` error: %s", j.Id, err).Error())
				return
			}
		}
		return
	}
}

func (s *Scheduler) _scheduleJob(ctx apsContext.Context, j job.Job) func() {
	return func() {
		s._runJob(ctx, j)
	}
}

// run 调度器
func (s *Scheduler) run(ctx context.Context) {
	for {
		select {
		case <-s.timer.C:
			ct := apsContext.NewContext()

			now := time.Now().UTC()
			nowi := now.Unix()
			js, err := s.GetDueJos(nowi)
			if err != nil {
				logs.DefaultLog.Error(ct, fmt.Sprintf("Scheduler get due jobs error: %s", err))
				s.timer.Reset(time.Second)
				continue
			}
			//wg := sync.WaitGroup{}
			for _, j := range js {
				var lockValue string = ""
				var lk = false
				taskCtx := apsContext.NewContext()

				if j.NextRunTime <= nowi {
					// 枷锁
					if s.distributed != nil {
						lk, err, lockValue = s.distributed.GetLock(taskCtx, j.Id)
						if err != nil {
							logs.DefaultLog.Error(taskCtx, "redis lock get failed", "error", err.Error())
							continue
						}
						if !lk {
							logs.DefaultLog.Info(taskCtx, "other process running", "jobId", j.Id)
							continue
						}
						//lockIds = append(lockIds, j.Id)
					}

					nextRunTime, isExpire, err := j.NextRunTimeHandler(taskCtx, nowi)
					if err != nil {
						logs.DefaultLog.Error(taskCtx, fmt.Sprintf("Scheduler calc next run time error: %s", err))
						continue
					}
					oldRunTime := j.NextRunTime
					j.NextRunTime = nextRunTime
					logs.DefaultLog.Info(taskCtx, "", "jobId", j.Id, "runTime", time.Unix(oldRunTime, 0).Format(time.RFC3339Nano), "next_run_time", time.Unix(j.NextRunTime, 0).Format(time.RFC3339Nano))

					// 更新任务会耗时 几十ms, 对其他任务有影响
					//wg.Add(1)
					// 先一步更新， 对于后续的 在任务重更新就没啥问题了
					//err = s.pool.Submit(s._flushJob(taskCtx, j, lockValue))
					//if err != nil {
					//	logs.DefaultLog.Error(taskCtx, "", "pool submit _flushJob", "error", err.Error(), "job", j)
					//}
					s._flushJob(taskCtx, j, lockValue)()
					if isExpire {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							Ctx:       taskCtx,
							EventCode: events.EVENT_JOB_MISSED,
							Job:       &j,
							Error:     errors.New(fmt.Sprintf("过期, Jitter:%s", j.Trigger.GetJitterTime())),
						}
						logs.DefaultLog.Info(taskCtx, "job expire jump this exec", "jobId", j.Id)
					} else if currentInstance := s.instances.Get(j.Id); currentInstance >= j.MaxInstance {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							Ctx:       taskCtx,
							EventCode: events.EVENT_MAX_INSTANCE,
							Job:       &j,
							Error:     errors.New(fmt.Sprintf("执行的任务数量超限, currentInstance: %d, jobMaxInstance: %d", currentInstance, j.MaxInstance)),
						}
						logs.DefaultLog.Info(taskCtx, "job max instance jump this exec", "jobId", j.Id)
					} else {
						err := s.pool.Submit(s._scheduleJob(taskCtx, j))
						if err != nil {
							logs.DefaultLog.Error(taskCtx, "pool submit _scheduleJob", "error", err.Error(), "job", j)
						}
						//logs.DefaultLog.Info(taskCtx, fmt.Sprintf("Job `%s` is running, at time: `%s`", j.Name, time.Unix(j.NextRunTime, 0).Format(time.RFC3339Nano)))
					}

				} else {
					break
				}
			}
			// wait job update completed
			//wg.Wait()
			s.jobChangeChan <- struct{}{}
		case <-ctx.Done():
			logs.DefaultLog.Info(context.Background(), "Scheduler quit.")
			return
		}
	}
}

// weakUp 设置定时器
func (s *Scheduler) weakUp(ctx context.Context) {
	for {
		select {
		case <-s.jobChangeChan:
			nextWakeupInterval, _ := s.getNextWakeupInterval()
			logs.DefaultLog.Info(context.Background(), fmt.Sprintf("Scheduler next wakeup interval %s", nextWakeupInterval))
			s.timer.Reset(nextWakeupInterval)
		case <-ctx.Done():
			logs.DefaultLog.Info(context.Background(), "WeakUp quit.")
			return
		}
	}
}

// GetDueJos 获取将要运行的任务
func (s *Scheduler) GetDueJos(timestamp int64) ([]job.Job, error) {
	jobs := make([]job.Job, 0)
	var errs = make([]error, 0)
	for _, store := range s.store {
		js, err := store.GetDueJobs(timestamp)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		jobs = append(jobs, js...)
	}
	return jobs, errors.Join(errs...)
}

// Start scheduler 开启运行
func (s *Scheduler) Start() {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()

	if s.isRunning {
		logs.DefaultLog.Info(context.Background(), "Scheduler is running.")
		return
	}

	s.timer = time.NewTimer(0)
	s.jobChangeChan = make(chan struct{}, 3)
	s.isRunning = true

	go s.weakUp(s.ctx)
	go s.run(s.ctx)
	logs.DefaultLog.Info(context.Background(), "Scheduler start.")
}

// Stop 停止scheduler
func (s *Scheduler) Stop() {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	defer s.pool.Release()

	if !s.isRunning {
		logs.DefaultLog.Info(context.Background(), "Scheduler has stopped.")
		return
	}
	s.cancel()
	s.isRunning = false
	logs.DefaultLog.Info(context.Background(), "Scheduler stop.")
}

// getNextWakeupInterval 获取定时器下次到期时间
func (s *Scheduler) getNextWakeupInterval() (time.Duration, bool) {
	var jobstore_next_run_time int64 = time.Now().UTC().Unix()
	var err error
	// 默认设置最大唤醒时间
	next_wakeup_time := triggers.MaxDate.UTC().Unix()
	for _, store := range s.store {
		jobstore_next_run_time, err = store.GetNextRunTime()
		if err != nil {
			logs.DefaultLog.Error(context.Background(), "Scheduler get next wakeup interval", "error", err.Error())
			jobstore_next_run_time = time.Now().UTC().Unix() + 1
		}
		if jobstore_next_run_time != 0 && jobstore_next_run_time < next_wakeup_time {
			next_wakeup_time = jobstore_next_run_time
		}
	}

	now := time.Now().UTC().Unix()
	nextWakeupInterval := next_wakeup_time - now
	if nextWakeupInterval <= 0 {
		nextWakeupInterval = 1
		//return 1, false
	}

	return time.Duration(nextWakeupInterval) * time.Second, true
}
