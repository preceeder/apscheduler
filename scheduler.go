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
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/triggers"
	"log/slog"
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
	// Input is received when `stop` is called or no job in store.
	quitChan chan struct{}
	// reset timer
	jobChangeChan chan struct{}
	// It should not be set manually.
	isRunning bool
	mutexS    sync.RWMutex
}

// NewScheduler 默认创建一个 MemoryStore
func NewScheduler() *Scheduler {
	var store map[string]stores.Store = map[string]stores.Store{stores.DefaultName: &stores.MemoryStore{}}
	// 设置 slog 日志
	logs.NewSlog()
	return &Scheduler{
		store:  store,
		mutexS: sync.RWMutex{},
	}
}

func (s *Scheduler) IsRunning() bool {
	s.mutexS.RLock()
	defer s.mutexS.RUnlock()

	return s.isRunning
}

// Bind the store
func (s *Scheduler) SetStore(name string, sto stores.Store) error {
	s.store[name] = sto
	if err := s.store[name].Init(); err != nil {
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
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	if j.Id == "" {
		return j, apsError.JobIdError("can not is \"\"")
	}
	if err := j.Init(); err != nil {
		return job.Job{}, err
	}

	slog.Info(fmt.Sprintf("Scheduler add job `%s`.", j.Name))
	// 要查询一下 所有的 村处理里有没有一样的任务id
	var jobExists bool
	// 检查任务是否已存在
	for k, st := range s.store {
		_, err := st.GetJob(j.Id)
		if err != nil {
			if errors.As(err, &apsError.JobNotFoundErrorType) {
				continue
			} else {
				return j, err
			}
		}

		if k != j.StoreName {
			return j, apsError.JobExistsError(fmt.Sprintf("%s exists other store:%s", j.Id, k))
		} else {
			jobExists = true
		}
	}
	//store, err := s.getStore(j.StoreName)
	//if err != nil {
	//	return job.Job{}, err
	//}
	//
	//_, err = store.GetJob(j.Id)
	if jobExists {
		// 存在且  任务可以更新
		if j.Replace {
			js, err := s._updateJob(j)
			if err != nil {
				return js, err
			}
			slog.Info("add job to update", "job", j)

		} else {
			return j, apsError.JobExistsError(fmt.Sprintf("%s exists %s, can't update", j.Id, j.StoreName))
		}
	} else {
		store, err := s.getStore(j.StoreName)
		if err != nil {
			return j, err
		}
		if err := store.AddJob(j); err != nil {
			return j, err
		}
		slog.Info("add job", "job", j)
	}
	//if err != nil {
	//	if errors.As(err, &apsError.JobNotFoundErrorType) {
	//		if err := store.AddJob(j); err != nil {
	//			return job.Job{}, err
	//		}
	//		slog.Info("add job", "job", j)
	//	} else {
	//		return j, err
	//	}
	//} else {
	//	// 存在 且 更新
	//	if j.Replace {
	//		js, err := s._updateJob(j)
	//		if err != nil {
	//			return js, err
	//		}
	//	}
	//}

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

func (s *Scheduler) GetAllJobs() ([]job.Job, error) {
	jobs := make([]job.Job, 0)
	errs := make([]error, 0)
	for _, store := range s.store {
		if job, err := store.GetAllJobs(); err == nil {
			jobs = append(jobs, job...)
		} else {
			errs = append(errs, err)
		}
	}
	return jobs, errors.Join(errs...)
}

// UpdateJob [job.Id, job.StoreName] 不能修改, 要修改job 可以线getJob, 然后update
func (s *Scheduler) UpdateJob(j job.Job) (job.Job, error) {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	err := j.Init()
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
	slog.Info("update job", "oldjob", oJ, "newJob", j)

	//if _, ok := s.getNextWakeupInterval(); !ok {
	//	s.wakeup()
	//}

	return j, nil
}

func (s *Scheduler) DeleteJob(id string, storeName string) error {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	err := s._deleteJob(id, storeName)
	if err != nil {
		return err
	}
	s.jobChangeChan <- struct{}{}
	return nil
}

func (s *Scheduler) _deleteJob(id string, storeName string) error {
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

func (s *Scheduler) DeleteAllJobs() error {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	slog.Info("delete all jobs.")
	errs := make([]error, 0)
	for _, store := range s.store {
		if err := store.DeleteAllJobs(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
	now := time.Now().UTC().Unix()
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
	now := time.Now().UTC().Unix()
	j.NextRunTime, _ = j.Trigger.GetNextRunTime(0, now)
	j, err = s._updateJob(j)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

// Used in standalone mode.
func (s *Scheduler) _runJob(j job.Job) {
	f := reflect.ValueOf(job.FuncMap[j.FuncName].Func)
	if f.IsNil() {
		slog.Warn(fmt.Sprintf("Job `%s` Func `%s` unregistered", j.Name, j.FuncName))
	} else {
		slog.Info(fmt.Sprintf("Job `%s` is running, next run time: `%d`", j.Name, j.NextRunTime))
		go func() {
			timeout, err := time.ParseDuration(j.Timeout)
			if err != nil {
				e := &apsError.JobTimeoutError{FullName: j.Name, Timeout: j.Timeout, Err: err}
				slog.Error(e.Error())
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			ch := make(chan error, 1)
			go func() {
				defer close(ch)
				defer func() {
					if err := recover(); err != nil {
						slog.Error(fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
						slog.Debug(fmt.Sprintf("%s", string(debug.Stack())))
					}
				}()

				f.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(j)})
			}()

			select {
			case <-ch:
				return
			case <-ctx.Done():
				slog.Warn(fmt.Sprintf("Job `%s` run timeout", j.Name))
			}
		}()
	}
}

func (s *Scheduler) _flushJob(j job.Job) error {
	if j.NextRunTime == 0 {
		if err := s._deleteJob(j.Id, j.StoreName); err != nil {
			return fmt.Errorf("delete job `%s` error: %s", j.Name, err)
		}
	} else {
		if _, err := s._updateJob(j); err != nil {
			return fmt.Errorf("update job `%s` error: %s", j.Name, err)
		}
	}
	return nil
}

func (s *Scheduler) _scheduleJob(j job.Job) error {
	// In standalone mode.
	s._runJob(j)
	return nil
}

func (s *Scheduler) RunJob(j job.Job) error {
	slog.Info(fmt.Sprintf("Scheduler run job `%s`.", j.Name))

	s._runJob(j)

	return nil
}

func (s *Scheduler) run() {
	for {
		select {
		case <-s.quitChan:
			slog.Info("Scheduler quit.")
			return
		case <-s.timer.C:
			now := time.Now().UTC()
			nowi := now.Unix()
			//js, err := s.GetAllJobs()
			js, err := s.GetDueJos(nowi)
			if err != nil {
				slog.Error(fmt.Sprintf("Scheduler get due jobs error: %s", err))
				s.timer.Reset(time.Second)
				continue
			}

			for _, j := range js {
				if j.NextRunTime <= nowi {
					nextRunTime, isExpire, err := j.NextRunTimeHandler(nowi)
					if err != nil {
						slog.Error(fmt.Sprintf("Scheduler calc next run time error: %s", err))
						continue
					}

					j.NextRunTime = nextRunTime

					if !isExpire {
						err = s._scheduleJob(j)
						if err != nil {
							slog.Error(fmt.Sprintf("Scheduler schedule job `%s` error: %s", j.Name, err))
							continue
						}
					} else {
						slog.Info("job expire jump this exec", "jobId", j.Id, "next_run_time", nextRunTime)
					}

					err = s._flushJob(j)
					if err != nil {
						slog.Error(fmt.Sprintf("Scheduler %s", err))
						continue
					}
				} else {
					break
				}
			}

			s.jobChangeChan <- struct{}{}

		case <-s.jobChangeChan:
			nextWakeupInterval, _ := s.getNextWakeupInterval()
			slog.Info(fmt.Sprintf("Scheduler next wakeup interval %s", nextWakeupInterval))

			s.timer.Reset(nextWakeupInterval)

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
	s.quitChan = make(chan struct{}, 3)
	s.jobChangeChan = make(chan struct{}, 3)
	s.isRunning = true

	go s.run()

	slog.Info("Scheduler start.")
}

func (s *Scheduler) Stop() {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()

	if !s.isRunning {
		slog.Info("Scheduler has stopped.")
		return
	}

	s.quitChan <- struct{}{}
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
			jobstore_next_run_time = time.Now().UTC().Unix() + 1
		}
		jobstore_next_run_time = jobstore_next_run_time * 1000
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
	//fmt.Println(nextWakeupInterval, time.Duration(nextWakeupInterval))

	return time.Duration(nextWakeupInterval * 1000000), true
}

func (s *Scheduler) wakeup() {
	if s.timer != nil {
		s.timer.Reset(0)
	}
}
