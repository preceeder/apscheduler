package stores

import (
	"bytes"
	"encoding/gob"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/job"
	"sort"
)

// Stores jobs in an array in RAM. Provides no persistence support.
type MemoryStore struct {
	jobs []job.Job
}

func (s *MemoryStore) Init() error {
	return nil
}

func (s *MemoryStore) Close() error {
	clear(s.jobs)
	return nil
}

func (s *MemoryStore) AddJob(j job.Job) error {
	s.jobs = append(s.jobs, j)
	sort.Sort(job.JobSlice(s.jobs))
	return nil
}

func (s *MemoryStore) GetJob(id string) (job.Job, error) {
	for _, j := range s.jobs {
		if j.Id == id {
			return j, nil
		}
	}
	return job.Job{}, apsError.JobNotFoundError(id)
}

func (s *MemoryStore) GetDueJobs(timestamp int64) ([]job.Job, error) {
	var dueIndex = -1
	for i, sj := range s.jobs {
		if sj.NextRunTime <= timestamp {
			dueIndex = i
		}
	}
	return s.jobs[0 : dueIndex+1], nil
}

func (s *MemoryStore) GetAllJobs() ([]job.Job, error) {
	return s.jobs, nil
}

func (s *MemoryStore) UpdateJob(j job.Job) error {
	for i, sj := range s.jobs {
		if sj.Id == j.Id {
			s.jobs[i] = j
			sort.Sort(job.JobSlice(s.jobs))
			return nil
		}
	}

	return apsError.JobNotFoundError(j.Id)
}

func (s *MemoryStore) DeleteJob(id string) error {
	for i, j := range s.jobs {
		if j.Id == id {
			//s.jobs = slices.Delete(s.jobs, i, i+1)
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			return nil
		}
	}
	return apsError.JobNotFoundError(id)
}

func (s *MemoryStore) DeleteAllJobs() error {
	s.jobs = nil
	return nil
}

func (s *MemoryStore) GetNextRunTime() (int64, error) {
	if len(s.jobs) == 0 {
		return 0, nil
	}
	return s.jobs[0].NextRunTime, nil
}

func (s *MemoryStore) Clear() error {
	return s.DeleteAllJobs()
}

func (s *MemoryStore) StateDump(j job.Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize Bytes and convert to Job
func (s *MemoryStore) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}
