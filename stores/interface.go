package stores

import (
	"github.com/preceeder/apscheduler/job"
)

// Defines the interface that each store must implement.
type Store interface {
	// Initialization functions for each store,
	// called when the scheduler run `SetStore`.
	Init() error

	// Add job to this store.
	AddJob(j job.Job) error

	// Get the job from this store.
	//  @return error `JobNotFoundError` if there are no job.
	GetJob(id string) (job.Job, error)

	// Get all jobs from this store.
	GetAllJobs() ([]job.Job, error)

	// 获取当前需要执行的任务
	GetDueJobs(timestamp int64) ([]job.Job, error)

	// Update job in store with a newer version.
	UpdateJob(j job.Job) error

	// Delete the job from this store.
	DeleteJob(id string) error

	// Delete all jobs from this store.
	DeleteAllJobs() error

	// Get the earliest next run time of all the jobs stored in this store,
	// or `time.Time{}` if there are no job.
	// Used to set the wakeup interval for the scheduler.
	GetNextRunTime() (int64, error)

	// Clear all resources bound to this store.
	Clear() error

	StateDump(j job.Job) ([]byte, error)
	StateLoad(state []byte) (job.Job, error)
}
