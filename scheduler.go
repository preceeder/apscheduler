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
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"log/slog"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type Scheduler struct {
	log slog.Logger
	// Job store
	store map[string]stores.Store
	// When the time is up, the scheduler will wake up.
	timer *time.Timer
	// reset timer
	jobChangeChan chan struct{}
	instances     map[string]int
	// It should not be set manually.
	isRunning bool
	mutexS    sync.RWMutex
	// Input is received when `stop` is called
	ctx    context.Context
	cancel context.CancelFunc
	pool   *ants.Pool // 默认10000,   会被 执行job 和 update job 平分调
}

// NewScheduler 默认创建一个 MemoryStore
func NewScheduler(logConfig ...logs.SlogConfig) *Scheduler {
	//var store map[string]stores.Store = map[string]stores.Store{stores.DefaultName: &stores.MemoryStore{}}
	// 设置 slog 日志
	logs.NewSlog(logConfig...)
	ctx, cancel := context.WithCancel(context.Background())

	events.StartEventsListen(ctx)
	pool, err := ants.NewPool(10000, ants.WithNonblocking(true))
	if err != nil {
		slog.Error("init pool failed", "error", err.Error())
		os.Exit(1)
	}
	return &Scheduler{
		store:     map[string]stores.Store{},
		mutexS:    sync.RWMutex{},
		instances: make(map[string]int),
		ctx:       ctx,
		cancel:    cancel,
		pool:      pool,
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

func (s *Scheduler) AddJob(j job.Job) (job.Job, error) {
	var err error
	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOB_ADDED,
			Job:       &j,
			Error:     err,
		}
	}()
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	if j.Id == "" {
		err = apsError.JobIdError("is can not empty")
		return j, err
	}
	if err = j.Init(); err != nil {
		return job.Job{}, err
	}

	slog.Info(fmt.Sprintf("Scheduler add job `%s`.", j.Name))
	// 要查询一下 所有的 存储器里有没有一样的任务id
	var jobExists bool
	// 检查任务是否已存在
	for k, st := range s.store {
		_, err = st.GetJob(j.Id)
		if err != nil {
			if errors.As(err, &apsError.JobNotFoundErrorType) {
				continue
			} else {
				return j, err
			}
		}

		if k != j.StoreName {
			err = apsError.JobExistsError(fmt.Sprintf("%s exists other store:%s", j.Id, k))
			return j, err
		} else {
			jobExists = true
		}
	}

	if jobExists {
		// 存在且  任务可以更新
		if j.Replace {
			js, err := s._updateJob(j)
			if err != nil {
				return js, err
			}
			slog.Info("add job to update", "job", j)

		} else {
			err = apsError.JobExistsError(fmt.Sprintf("%s exists %s, can't update", j.Id, j.StoreName))
			return j, err
		}
	} else {
		store, err := s.getStore(j.StoreName)
		if err != nil {
			return j, err
		}
		if err = store.AddJob(j); err != nil {
			return j, err
		}
		slog.Info("add job", "job", j)
	}

	if s.isRunning {
		s.jobChangeChan <- struct{}{}
	}
	return j, nil
}

func (s *Scheduler) GetJob(id string, storeName string) (job.Job, error) {
	store, err := s.getStore(storeName)
	if err != nil {
		return job.Job{}, err
	}
	return store.GetJob(id)
}

// QueryJob 查询job 没有storeName
func (s *Scheduler) QueryJob(id string) (job.Job, error) {
	errs := make([]error, 0)
	for _, store := range s.store {
		if jb, err := store.GetJob(id); err == nil {
			return jb, errors.Join(errs...)
		} else {
			if errors.As(err, &apsError.JobNotFoundErrorType) {
				continue
			}
			errs = append(errs, err)
		}
	}
	return job.Job{}, errors.Join(errs...)
}

func (s *Scheduler) GetAllJobs() ([]job.Job, error) {
	jobs := make([]job.Job, 0)
	errs := make([]error, 0)
	for _, store := range s.store {
		if jb, err := store.GetAllJobs(); err == nil {
			jobs = append(jobs, jb...)
		} else {
			errs = append(errs, err)
		}
	}
	return jobs, errors.Join(errs...)
}

// UpdateJob [job.Id, job.StoreName] 不能修改, 要修改job 可以线getJob, 然后update
func (s *Scheduler) UpdateJob(j job.Job) (job.Job, error) {
	var err error
	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOB_MODIFIED,
			Job:       &j,
			Error:     err,
		}
	}()

	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	err = j.Init()
	if err != nil {
		return j, err
	}
	js, err := s._updateJob(j)
	if err != nil {
		return js, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) _updateJob(j job.Job) (job.Job, error) {

	oJ, err := s.GetJob(j.Id, j.StoreName)
	if err != nil {
		return job.Job{}, err
	}

	if j.Status == "" ||
		(j.Status != job.STATUS_RUNNING && j.Status != job.STATUS_PAUSED) {
		j.Status = oJ.Status
	}

	if err := j.Check(); err != nil {
		return job.Job{}, err
	}

	store, err := s.getStore(j.StoreName)
	if err != nil {
		return j, err
	}

	if err := store.UpdateJob(j); err != nil {
		return job.Job{}, err
	}

	//slog.Info("update job", "oldjob", oJ, "newJob", j)

	return j, nil
}

func (s *Scheduler) DeleteJob(id string, storeName string) (err error) {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	err = s._deleteJob(id, storeName)
	if err != nil {
		return err
	}
	s.jobChangeChan <- struct{}{}
	return nil
}

func (s *Scheduler) _deleteJob(id string, storeName string) (err error) {
	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOB_REMOVED,
			Job:       &job.Job{Id: id, StoreName: storeName},
			Error:     err,
		}
	}()
	slog.Info("delete job", "jobId", id)
	if _, err := s.GetJob(id, storeName); err != nil {
		return err
	}
	store, err := s.getStore(storeName)
	if err != nil {
		return err
	}
	return store.DeleteJob(id)
}

func (s *Scheduler) DeleteAllJobs() (err error) {
	var storeNames string
	defer func() {
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_ALL_JOBS_REMOVED,
			Error:     err,
			Msg:       storeNames,
		}
	}()

	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	slog.Info("delete all jobs.")
	errs := make([]error, 0)
	var storeNameSlice = make([]string, 0)
	for stn, store := range s.store {
		storeNameSlice = append(storeNameSlice, stn)
		if err = store.DeleteAllJobs(); err != nil {
			errs = append(errs, err)
		}
	}
	storeNames = strings.Join(storeNameSlice, ",")
	err = errors.Join(errs...)
	return
}

func (s *Scheduler) PauseJob(id string, storeName string) (job.Job, error) {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	slog.Info("pause job", "jobId", id)

	j, err := s.GetJob(id, storeName)
	if err != nil {
		return job.Job{}, err
	}

	j.Status = job.STATUS_PAUSED
	now := time.Now().UTC().UnixMilli()
	j.NextRunTime, _ = j.Trigger.GetNextRunTime(0, now)
	j, err = s._updateJob(j)
	if err != nil {
		return job.Job{}, err
	}

	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) ResumeJob(id string, storeName string) (job.Job, error) {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	slog.Info("Scheduler resume job", "jobId", id)

	j, err := s.GetJob(id, storeName)
	if err != nil {
		return job.Job{}, err
	}

	j.Status = job.STATUS_RUNNING
	now := time.Now().UTC().UnixMilli()
	j.NextRunTime, _ = j.Trigger.GetNextRunTime(0, now)
	j, err = s._updateJob(j)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) _runJob(j job.Job) {
	f := reflect.ValueOf(job.FuncMap[j.FuncName].Func)
	if f.IsNil() {
		msg := fmt.Sprintf("Job `%s` Func `%s` unregistered", j.Name, j.FuncName)
		events.EventChan <- events.EventInfo{
			EventCode: events.EVENT_JOB_ERROR,
			Job:       &j,
			Error:     errors.New(msg),
		}
		slog.Warn(msg)
	} else {
		slog.Info(fmt.Sprintf("Job `%s` is running, at time: `%s`", j.Name, time.UnixMilli(j.NextRunTime).Format(time.RFC3339Nano)))
		s.instances[j.Id] += 1
		defer func() {
			s.instances[j.Id] -= 1
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(j.Timeout))
		defer cancel()

		ch := make(chan error, 1)
		go func() {
			defer close(ch)
			defer func() {
				if err := recover(); err != nil {
					slog.Error(fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
					slog.Debug(fmt.Sprintf("%s", string(debug.Stack())))
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
			slog.Warn(err)
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
				slog.Error(fmt.Errorf("Scheduler delete job `%s` error: %s", j.Id, err).Error())
				return
			}
		} else {
			if _, err := s._updateJob(j); err != nil {
				slog.Error(fmt.Errorf("Scheduler update job `%s` error: %s", j.Id, err).Error())

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

func (s *Scheduler) RunJob(j job.Job) error {
	slog.Info(fmt.Sprintf("Scheduler run job `%s`.", j.Name))
	s._runJob(j)

	return nil
}

func (s *Scheduler) run(ctx context.Context) {
	for {
		select {
		case <-s.timer.C:
			now := time.Now().UTC()
			nowi := now.UnixMilli()
			js, err := s.GetDueJos(nowi)
			if err != nil {
				slog.Error(fmt.Sprintf("Scheduler get due jobs error: %s", err))
				s.timer.Reset(time.Second)
				continue
			}

			wg := sync.WaitGroup{}
			for _, j := range js {
				if j.NextRunTime <= nowi {
					nextRunTime, isExpire, err := j.NextRunTimeHandler(nowi)
					if err != nil {
						slog.Error(fmt.Sprintf("Scheduler calc next run time error: %s", err))
						continue
					}

					if isExpire {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							EventCode: events.EVENT_JOB_MISSED,
							Job:       &j,
							Error:     errors.New("过期"),
						}
						slog.Info("job expire jump this exec", "jobId", j.Id)

					} else if s.instances[j.Id] >= j.MaxInstance {
						// job 本次不执行
						events.EventChan <- events.EventInfo{
							EventCode: events.EVENT_JOB_MISSED,
							Job:       &j,
							Error:     errors.New("执行的任务数量超限"),
						}
						slog.Info("job max instance jump this exec", "jobId", j.Id)
					} else {
						err := s.pool.Submit(s._scheduleJob(j))
						if err != nil {
							slog.ErrorContext(ctx, "pool submit _scheduleJob", "error", err.Error(), "job", j)
						}
						//err = s._scheduleJob(j)
						//if err != nil {
						//	slog.Error(fmt.Sprintf("Scheduler schedule job `%s` error: %s", j.Name, err))
						//	continue
						//}
					}

					j.NextRunTime = nextRunTime
					slog.Info("", "jobId", j.Id, "next_run_time", time.UnixMilli(j.NextRunTime).Format(time.RFC3339Nano), "timestamp", j.NextRunTime)
					// 更新任务会耗时 几十ms, 对其他任务有影响
					err = s.pool.Submit(s._flushJob(j, &wg))
					wg.Add(1)
					if err != nil {
						slog.ErrorContext(ctx, "pool submit _flushJob", "error", err.Error(), "job", j)
					}
					//err = s._flushJob(j)
					//if err != nil {
					//	slog.Error(fmt.Sprintf("Scheduler %s", err))
					//	continue
					//}
				} else {
					break
				}
			}
			// wait job update completed
			wg.Wait()
			s.jobChangeChan <- struct{}{}
		case <-ctx.Done():
			slog.Info("Scheduler quit.")

			break
		}
	}
}

func (s *Scheduler) weakUp(ctx context.Context) {
	for {
		select {
		case <-s.jobChangeChan:
			nextWakeupInterval, _ := s.getNextWakeupInterval()
			slog.Info(fmt.Sprintf("Scheduler next wakeup interval %s", nextWakeupInterval))

			s.timer.Reset(nextWakeupInterval)
		case <-ctx.Done():
			break
		}
	}
}

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

func (s *Scheduler) Start() {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	if s.isRunning {
		slog.Info("Scheduler is running.")
		return
	}

	s.timer = time.NewTimer(0)
	s.jobChangeChan = make(chan struct{}, 3)
	s.isRunning = true

	go s.weakUp(s.ctx)
	go s.run(s.ctx)

	slog.Info("Scheduler start.")
}

func (s *Scheduler) Stop() {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	defer s.pool.Release()

	if !s.isRunning {
		slog.Info("Scheduler has stopped.")
		return
	}
	s.cancel()
	s.isRunning = false

	slog.Info("Scheduler stop.")
}

func (s *Scheduler) getNextWakeupInterval() (time.Duration, bool) {
	var jobstore_next_run_time int64 = time.Now().UTC().UnixMilli()
	var err error
	// 默认设置最大唤醒时间
	next_wakeup_time := triggers.MaxDate.UTC().UnixMilli()
	for _, store := range s.store {
		jobstore_next_run_time, err = store.GetNextRunTime()
		if err != nil {
			slog.Error(fmt.Sprintf("Scheduler get next wakeup interval error: %s", err))
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

func (s *Scheduler) wakeup() {
	if s.timer != nil {
		s.timer.Reset(0)
	}
}
