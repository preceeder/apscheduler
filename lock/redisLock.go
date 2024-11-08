package lock

import (
	"context"
	"github.com/duke-git/lancet/v2/cryptor"
	"github.com/preceeder/apscheduler/common"
	"github.com/preceeder/apscheduler/logs"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

type RedisLock struct {
	redisClient *redis.Client
	lockPrefix  string // 分布式锁前缀
}

// 删除锁
var del = `
	if redis.call("get",KEYS[1]) == ARGV[1] then
		return redis.call("del",KEYS[1])
	else
		return 0
	end`

var delScript = cryptor.Sha1(del)

func checkLua(redisClient *redis.Client) error {
	ctx := context.Background()
	ns, err := redisClient.ScriptExists(ctx, delScript).Result()
	if err != nil {
		return err
	}
	if ns == nil || !ns[0] {
		_, err = redisClient.ScriptLoad(ctx, del).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

// lockPrefix = "go.apscheduler.lock"
func NewRedisLock(redisClient *redis.Client, lockPrefix string) (*RedisLock, error) {
	err := checkLua(redisClient)
	if err != nil {
		return nil, err
	}
	if lockPrefix == "" {
		lockPrefix = "go.apscheduler.lock"
	}
	return &RedisLock{
		redisClient: redisClient,
		lockPrefix:  lockPrefix,
	}, nil
}

func (s *RedisLock) GetLock(ctx context.Context, jobId string) (bool, error, string) {
	// exp  ms
	lockKey := strings.Join([]string{s.lockPrefix, jobId}, ":")
	value := common.RandStr(10)
	res, err := s.redisClient.SetNX(ctx, lockKey, value, time.Millisecond*500).Result()
	if err != nil {
		logs.DefaultLog.Error(ctx, "redisDb 加锁失败", "key", jobId, "error", err.Error())
		return false, err, ""
	}
	return res, nil, value
}

func (s *RedisLock) ReleaseLock(ctx context.Context, jobId string, value string) error {
	lockKey := strings.Join([]string{s.lockPrefix, jobId}, ":")
	rcmd := s.redisClient.EvalSha(ctx, delScript, []string{lockKey}, value)
	return rcmd.Err()
}
