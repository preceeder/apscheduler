//   File Name:  scheduler_api.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/6/12 17:16
//    Change Activity:

package apscheduler

import (
	"errors"
	"fmt"
	"github.com/preceeder/apscheduler/apsContext"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/events"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/logs"
	"github.com/preceeder/apscheduler/stores"
	"strings"
	"time"
)

func (s *Scheduler) AddJob(j job.Job) (job.Job, error) {
	var err error
	ctx := apsContext.NewContext()
	defer func() {
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_JOB_ADDED,
			Job:       &j,
			Error:     err,
		}
	}()

	if j.Id == "" {
		err = apsError.JobIdError("is can not empty")
		return j, err
	}
	if err = j.Init(); err != nil {
		return job.Job{}, err
	}

	logs.DefaultLog.Info(ctx, fmt.Sprintf("Scheduler add job `%s`.", j.Name))
	// 要查询一下 所有的 存储器里有没有一样的任务id
	var jobExists bool
	// 检查任务是否已存在
	for k, st := range s.store {
		_, err = st.GetJob(j.Id)
		if err != nil {
			if errors.As(err, &apsError.JobNotFoundErrorType) {
				err = nil
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
			js, err := s._updateJob(ctx, j, j.StoreName, true)
			if err != nil {
				return js, err
			}
			logs.DefaultLog.Info(ctx, "add job to update", "job", j)
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
		logs.DefaultLog.Info(ctx, "add job", "job", j)
	}

	if s.isRunning {
		s.jobChangeChan <- struct{}{}
	}
	return j, nil
}

// 删除job

func (s *Scheduler) DeleteJobByStoreName(id string, storeName string) (err error) {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	ctx := apsContext.NewContext()
	err = s._deleteJob(ctx, id, storeName)
	if err != nil {
		return err
	}
	s.jobChangeChan <- struct{}{}
	return nil
}

func (s *Scheduler) DeleteJob(id string) error {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	defer func() {
		s.jobChangeChan <- struct{}{}
	}()
	ctx := apsContext.NewContext()
	logs.DefaultLog.Info(ctx, "delete job", "jobId", id)

	for _, store := range s.store {
		if jb, err := store.GetJob(id); err == nil {
			err = store.DeleteJob(id)
			events.EventChan <- events.EventInfo{
				Ctx:       ctx,
				EventCode: events.EVENT_JOB_REMOVED,
				Job:       &jb,
				Error:     err,
			}
			return err
		}
	}
	return nil
}

// DeleteJobsByStoreName 删除指定store 下所有的job
func (s *Scheduler) DeleteJobsByStoreName(storeName string) (err error) {
	ctx := apsContext.NewContext()
	defer func() {
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_ALL_JOBS_REMOVED,
			Error:     err,
			Msg:       storeName,
		}
	}()

	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	logs.DefaultLog.Info(ctx, "delete all jobs.", "storeName", storeName)

	store, err := s.getStore(storeName)
	if err != nil {
		return
	}
	if err = store.DeleteAllJobs(); err != nil {
		return
	}

	return
}

func (s *Scheduler) DeleteAllJobs() (err error) {
	var storeNames string
	ctx := apsContext.NewContext()
	defer func() {
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_ALL_JOBS_REMOVED,
			Error:     err,
			Msg:       storeNames,
		}
	}()

	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	logs.DefaultLog.Info(ctx, "delete all jobs.")

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

func (s *Scheduler) _deleteJob(ctx apsContext.Context, id string, storeName string) (err error) {
	var j job.Job = job.Job{
		Id: id, StoreName: storeName,
	}
	defer func() {
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_JOB_REMOVED,
			Job:       &j,
			Error:     err,
		}
	}()
	logs.DefaultLog.Info(ctx, "delete job", "jobId", id)

	if jb, err := s.QueryJobByStoreName(id, storeName); err != nil {
		return err
	} else {
		j = jb
	}
	store, err := s.getStore(storeName)
	if err != nil {
		return err
	}
	return store.DeleteJob(id)
}

// 查询job

// QueryJobByStoreName 更具指定的 store name 去获取
func (s *Scheduler) QueryJobByStoreName(id string, storeName string) (job.Job, error) {
	store, err := s.getStore(storeName)
	if err != nil {
		return job.Job{}, err
	}
	return store.GetJob(id)
}

// QueryJob 查询job 没有storeName
func (s *Scheduler) QueryJob(id string) (job.Job, error) {
	j, _, err := s.queryJobById(id)
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}

// GetJobsByStoreName 获取指定 store 下所有的job
func (s *Scheduler) GetJobsByStoreName(storeName string) ([]job.Job, error) {
	store, err := s.getStore(storeName)
	if err != nil {
		return nil, err
	}
	if jb, err := store.GetAllJobs(); err != nil {
		return nil, err
	} else {
		return jb, nil
	}
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

// UpdateJob [job.Id, job.StoreName] 不能修改, 要修改job 可以先getJob, 然后update
// 可以将 job换到新的store中, 当oldJobStoreName 和 j.StoreName 不一样的时候 更新后会删除旧store中的job
// oldJobStoreName string 旧job 存储的store name,
func (s *Scheduler) UpdateJob(j job.Job, oldJobStoreName string) (job.Job, error) {
	var err error
	ctx := apsContext.NewContext()
	defer func() {
		events.EventChan <- events.EventInfo{
			Ctx:       ctx,
			EventCode: events.EVENT_JOB_MODIFIED,
			Job:       &j,
			Error:     err,
		}
	}()

	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	err = j.Init()
	if err != nil {
		return j, err
	}
	js, err := s._updateJob(ctx, j, oldJobStoreName, true)
	if err != nil {
		return js, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

// _updateJob 更新 job
// j 最新的job数据
// oldJobStoreName job之前存在的 store
func (s *Scheduler) _updateJob(ctx apsContext.Context, j job.Job, oldJobStoreName string, forceUpdate bool) (job.Job, error) {
	// 如果让用户自己操作  就可以更加灵活
	oJ, err := s.QueryJobByStoreName(j.Id, oldJobStoreName)
	if err != nil {
		return job.Job{}, err
	}

	if j.Status == "" ||
		(j.Status != job.STATUS_RUNNING && j.Status != job.STATUS_PAUSED) {
		j.Status = oJ.Status
	}

	if err = j.Check(); err != nil {
		return job.Job{}, err
	}

	// 这里是防止在获取到任务后，更新任务之前 有外部操作更新了任务导致，本次更新覆盖之前的有效更新
	if forceUpdate == false && oJ.HashValue != "" && oJ.HashValue != j.HashValue {
		return job.Job{}, errors.New("old job hashValue has changed")
	}

	store, err := s.getStore(j.StoreName)
	if err != nil {
		return j, err
	}

	if err = store.UpdateJob(j); err != nil {
		return job.Job{}, err
	}
	if j.StoreName != oldJobStoreName {
		err = s._deleteJob(ctx, j.Id, oldJobStoreName)
		if err != nil {
			return job.Job{}, err
		}
	}

	return j, nil
}

// 暂停job

// PauseJobByStoreName 暂停指定store name 下的job
func (s *Scheduler) PauseJobByStoreName(id string, storeName string) (job.Job, error) {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	ctx := apsContext.NewContext()
	logs.DefaultLog.Info(ctx, "pause job", "jobId", id)

	j, err := s.QueryJobByStoreName(id, storeName)
	if err != nil {
		return job.Job{}, err
	}

	j.Status = job.STATUS_PAUSED
	now := time.Now().Add(time.Hour * 24 * 365 * 100).UTC().Unix()
	j.NextRunTime = now
	j, err = s._updateJob(ctx, j, j.StoreName, true)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) PauseJob(id string) (job.Job, error) {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	ctx := apsContext.NewContext()
	logs.DefaultLog.Info(ctx, "pause job", "jobId", id)

	j, _, err := s.queryJobById(id)
	if err != nil {
		return job.Job{}, err
	}
	j.Status = job.STATUS_PAUSED
	now := time.Now().Add(time.Hour * 24 * 365 * 100).UTC().Unix()
	j.NextRunTime = now
	j, err = s._updateJob(ctx, j, j.StoreName, true)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

// ResumeJobByStoreName
// job 恢复运行
func (s *Scheduler) ResumeJobByStoreName(id string, storeName string) (job.Job, error) {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	ctx := apsContext.NewContext()

	logs.DefaultLog.Info(ctx, "Scheduler resume job", "jobId", id)

	j, err := s.QueryJobByStoreName(id, storeName)
	if err != nil {
		return job.Job{}, err
	}

	j.Status = job.STATUS_RUNNING
	now := time.Now().UTC().Unix()
	j.NextRunTime, _ = j.Trigger.GetNextRunTime(0, now)
	j, err = s._updateJob(ctx, j, j.StoreName, true)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) ResumeJob(id string) (job.Job, error) {
	//s.mutexS.Lock()
	//defer s.mutexS.Unlock()
	ctx := apsContext.NewContext()

	logs.DefaultLog.Info(ctx, "Scheduler resume job", "jobId", id)

	j, _, err := s.queryJobById(id)
	if err != nil {
		return job.Job{}, err
	}

	j.Status = job.STATUS_RUNNING
	now := time.Now().UTC().Unix()
	j.NextRunTime, _ = j.Trigger.GetNextRunTime(0, now)
	j, err = s._updateJob(ctx, j, j.StoreName, true)
	if err != nil {
		return job.Job{}, err
	}
	s.jobChangeChan <- struct{}{}
	return j, nil
}

func (s *Scheduler) queryJobById(id string) (job.Job, stores.Store, error) {
	for _, store := range s.store {
		if jb, err := store.GetJob(id); err == nil {
			return jb, store, nil
		}
	}
	return job.Job{}, nil, apsError.JobNotFoundError(id)
}
