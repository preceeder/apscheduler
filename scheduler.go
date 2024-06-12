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
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"os"
	"reflect"
	"runtime/debug"
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
	ctx    context.Context
	cancel context.CancelFunc
	pool   *ants.Pool // 默认10000,   会被 执行job 和 update job 平分
}

// NewScheduler 默认创建一个 MemoryStore
func NewScheduler() *Scheduler {
	//var store map[string]stores.Store = map[string]stores.Store{stores.DefaultName: &stores.MemoryStore{}}
	ctx, cancel := context.WithCancel(context.Background())

	events.StartEventsListen(ctx)
	pool, err := ants.NewPool(10000, ants.WithNonblocking(true))
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
		ctx:    ctx,
		cancel: cancel,
		pool:   pool,
	}
}

func (s *Scheduler) IsRunning() bool {
	s.mutexS.RLock()
	defer s.mutexS.RUnlock()

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

func (s *Scheduler) _runJob(j job.Job) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("hahaahah")
			logs.DefaultLog.Error(context.Background(), fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
			//logs.DefaultLog.Debug(context.Background(), fmt.Sprintf("%s", string(debug.Stack())))
		}
	}()
	f := reflect.ValueOf(job.FuncMap[j.FuncName].Func)
	if f.IsNil() {
		msg := fmt.Sprintf("Job `%s` Func `%s` unregistered", j.Name, j.FuncName)
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOB_ERROR,
			Job:       &j,
			Error:     errors.New(msg),
		}
		logs.DefaultLog.Warn(context.Background(), msg)
	} else {
		logs.DefaultLog.Info(context.Background(), fmt.Sprintf("Job `%s` is running, at time: `%s`", j.Name, time.UnixMilli(j.NextRunTime).Format(time.RFC3339Nano)))
		s.instances.Add(j.Id, 1)
		defer func() {
			s.instances.Sub(j.Id, 1)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(j.Timeout))
		defer cancel()

		ch := make(chan error, 1)
		go func() {
			defer close(ch)
			defer func() {
				if err := recover(); err != nil {
					logs.DefaultLog.Error(context.Background(), fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
					logs.DefaultLog.Debug(context.Background(), fmt.Sprintf("%s", string(debug.Stack())))
					events.EventChan <- events.EventInfo{
						EventCode: events.EVENT_JOB_ERROR,
						Job:       &j,
						Error:     err.(error),
					}
				}
			}()

			f.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(j)})
		}()

		select {
		case <-ch:
			return
		case <-ctx.Done():
			err := fmt.Sprintf("Job `%s` run timeout", j.Name)
			logs.DefaultLog.Warn(context.Background(), err)

			events.EventChan <- events.EventInfo{
				EventCode: events.EVENT_JOB_ERROR,
				Job:       &j,
				Error:     errors.New(err),
			}
		}

	}
}

func (s *Scheduler) _flushJob(j job.Job, wg *sync.WaitGroup) func() {
	return func() {
		defer wg.Done()
		if j.NextRunTime == 0 {
			if err := s._deleteJob(j.Id, j.StoreName); err != nil {
				logs.DefaultLog.Error(context.Background(), fmt.Errorf("Scheduler delete job `%s` error: %s", j.Id, err).Error())
				return
			}
		} else {
			if _, err := s._updateJob(j, j.StoreName); err != nil {
				logs.DefaultLog.Error(context.Background(), fmt.Errorf("Scheduler update job `%s` error: %s", j.Id, err).Error())
				return
			}
		}
		return
	}

}

func (s *Scheduler) _scheduleJob(j job.Job) func() {
	return func() {
		s._runJob(j)
	}
}

// run 调度器
func (s *Scheduler) run(ctx context.Context) {
	for {
		select {
		case <-s.timer.C:
			now := time.Now().UTC()
			nowi := now.UnixMilli()
			js, err := s.GetDueJos(nowi)
			if err != nil {
				logs.DefaultLog.Error(context.Background(), fmt.Sprintf("Scheduler get due jobs error: %s", err))
				s.timer.Reset(time.Second)
				continue
			}
			ct := context.Background()
			wg := sync.WaitGroup{}
			for _, j := range js {
				if j.NextRunTime <= nowi {
					nextRunTime, isExpire, err := j.NextRunTimeHandler(nowi)
					if err != nil {
						logs.DefaultLog.Error(ct, fmt.Sprintf("Scheduler calc next run time error: %s", err))
						continue
					}

					if isExpire {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							EventCode: events.EVENT_JOB_MISSED,
							Job:       &j,
							Error:     errors.New("过期"),
						}
						logs.DefaultLog.Info(ct, "job expire jump this exec", "jobId", j.Id)

					} else if s.instances.Get(j.Id) >= j.MaxInstance {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							EventCode: events.EVENT_JOB_MISSED,
							Job:       &j,
							Error:     errors.New("执行的任务数量超限"),
						}
						logs.DefaultLog.Info(ct, "job max instance jump this exec", "jobId", j.Id)
					} else {
						err := s.pool.Submit(s._scheduleJob(j))
						if err != nil {
							logs.DefaultLog.Error(ct, "pool submit _scheduleJob", "error", err.Error(), "job", j)
						}
					}

					j.NextRunTime = nextRunTime
					logs.DefaultLog.Info(ct, "", "jobId", j.Id, "next_run_time", time.UnixMilli(j.NextRunTime).Format(time.RFC3339Nano), "timestamp", j.NextRunTime)
					// 更新任务会耗时 几十ms, 对其他任务有影响
					wg.Add(1)
					err = s.pool.Submit(s._flushJob(j, &wg))
					if err != nil {
						logs.DefaultLog.Error(ct, "", "pool submit _flushJob", "error", err.Error(), "job", j)
					}
				} else {
					break
				}
			}
			// wait job update completed
			wg.Wait()
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
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

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
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
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
	var jobstore_next_run_time int64 = time.Now().UTC().UnixMilli()
	var err error
	// 默认设置最大唤醒时间
	next_wakeup_time := triggers.MaxDate.UTC().UnixMilli()
	for _, store := range s.store {
		jobstore_next_run_time, err = store.GetNextRunTime()
		if err != nil {
			logs.DefaultLog.Error(context.Background(), "Scheduler get next wakeup interval", "error", err.Error())
			jobstore_next_run_time = time.Now().UTC().UnixMilli() + 100
		}
		if jobstore_next_run_time != 0 && jobstore_next_run_time < next_wakeup_time {
			next_wakeup_time = jobstore_next_run_time
		}
	}

	now := time.Now().UTC().UnixMilli()
	nextWakeupInterval := next_wakeup_time - now
	if nextWakeupInterval < 0 {
		//nextWakeupInterval = time.Second
		return 0, false
	}

	return time.Duration(nextWakeupInterval) * time.Millisecond, true
}
