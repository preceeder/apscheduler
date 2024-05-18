package stores

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/job"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

const (
	JOBS_KEY      = "go.apscheduler.jobs"
	RUN_TIMES_KEY = "go.apscheduler.run_times"
)

type RedisConfig struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Password    string `json:"password"`
	Db          int    `json:"db"`
	MaxIdle     int    `json:"maxIdle"`
	IdleTimeout int    `json:"idleTimeout"`
	PoolSize    int    `json:"PoolSize"`
}

// Stores jobs in a Redis database.
type RedisStore struct {
	RedisConfig RedisConfig
	RDB         *redis.Client
	JobsKey     string
	RunTimesKey string
}

func NewRedisStore(config RedisConfig, JobsKey, RunTimesKey string) *RedisStore {
	rdb := initRedis(config)
	return &RedisStore{
		RedisConfig: config,
		RDB:         rdb,
		JobsKey:     JobsKey,
		RunTimesKey: RunTimesKey,
	}
}

func initRedis(cf RedisConfig) *redis.Client {
	addr := cf.Host + ":" + cf.Port
	redisOpt := &redis.Options{
		Addr:         addr,
		Password:     cf.Password,
		DB:           cf.Db,
		PoolSize:     cf.PoolSize,
		MaxIdleConns: cf.MaxIdle,
		MinIdleConns: cf.MaxIdle,
	}
	rdb := redis.NewClient(redisOpt)
	cmd := rdb.Ping(context.Background())
	if cmd.Err() != nil {
		panic("redis connect fail")
	}

	return rdb
}

func (s *RedisStore) Init() error {
	if s.JobsKey == "" {
		s.JobsKey = JOBS_KEY
	}
	if s.RunTimesKey == "" {
		s.RunTimesKey = RUN_TIMES_KEY
	}

	return nil
}

func (s *RedisStore) AddJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}

	_, err = s.RDB.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, s.JobsKey, j.Id, state)
		pipe.ZAdd(ctx, s.RunTimesKey, redis.Z{Score: float64(j.NextRunTime.UTC().Unix()), Member: j.Id})
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStore) GetJob(id string) (job.Job, error) {
	state, err := s.RDB.HGet(ctx, s.JobsKey, id).Bytes()
	if err == redis.Nil {
		return job.Job{}, apsError.JobNotFoundError(id)
	}
	if err != nil {
		return job.Job{}, err
	}

	return s.StateLoad(state)
}

func (s *RedisStore) GetAllJobs() ([]job.Job, error) {
	mapStates, err := s.RDB.HGetAll(ctx, s.JobsKey).Result()
	if err != nil {
		return nil, err
	}

	var jobList []job.Job
	for _, v := range mapStates {
		j, err := s.StateLoad([]byte(v))
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, j)
	}

	return jobList, nil
}

// GetDueJobs 还需要测试
func (s *RedisStore) GetDueJobs(now time.Time) ([]job.Job, error) {
	var jobList []job.Job

	Min := "-inf"
	Max := strconv.FormatInt(now.UTC().Unix(), 10)
	jobIds, err := s.RDB.ZRangeByScore(ctx, s.RunTimesKey, &redis.ZRangeBy{Min: Min, Max: Max}).Result()
	if err != nil {
		return jobList, err
	}
	if len(jobIds) > 0 {
		mapStates, err := s.RDB.HMGet(ctx, s.JobsKey, jobIds...).Result()
		if err != nil {
			return jobList, err
		}

		for _, v := range mapStates {
			j, err := s.StateLoad([]byte(v.(string)))
			if err != nil {
				return nil, err
			}
			jobList = append(jobList, j)
		}
		return jobList, nil
	}

	return jobList, nil
}

func (s *RedisStore) UpdateJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}

	_, err = s.RDB.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, s.JobsKey, j.Id, state)
		pipe.ZAdd(ctx, s.RunTimesKey, redis.Z{Score: float64(j.NextRunTime.UTC().Unix()), Member: j.Id})
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStore) DeleteJob(id string) error {
	_, err := s.RDB.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HDel(ctx, s.JobsKey, id)
		pipe.ZRem(ctx, s.RunTimesKey, id)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStore) DeleteAllJobs() error {
	_, err := s.RDB.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, s.JobsKey)
		pipe.Del(ctx, s.RunTimesKey)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisStore) GetNextRunTime() (time.Time, error) {
	sliceRunTimes, err := s.RDB.ZRangeWithScores(ctx, s.RunTimesKey, 0, 0).Result()
	if err != nil || len(sliceRunTimes) == 0 {
		return time.Time{}, nil
	}

	nextRunTimeMin := time.Unix(int64(sliceRunTimes[0].Score), 0).UTC()
	return nextRunTimeMin, nil
}

func (s *RedisStore) Clear() error {
	return s.DeleteAllJobs()
}

func (s *RedisStore) StateDump(j job.Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize Bytes and convert to Job
func (s *RedisStore) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}
