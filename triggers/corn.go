//   File Name:  corn.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import (
	"fmt"
	"github.com/gorhill/cronexpr"
	"time"
)

type CronTrigger struct {
	CronExpr     string `json:"cron_expr"`
	TimeZoneName string `json:"time_zone_name"` // 为空默认就是 UTC
	StartTime    string `json:"start_time"`     // time.DateTime
	EndTime      string `json:"end_time"`       // time.DateTime
	ExpireTime   int64  `json:"expire_time"`    // 任务超过多长时间就是过期, 过期则本次不执行 单位 time.Second, 也是误差值, 一般情况拿到这个任务做判定的时候 now > NextRunTime 比较微小的值

	startTime  int64
	startTimet time.Time
	endTime    int64
	timeZone   *time.Location
	isInit     bool
}

// GetLocation 获取时区
func (ct *CronTrigger) GetLocation() (err error) {
	ct.timeZone, err = time.LoadLocation(ct.TimeZoneName)
	if err != nil {
		return err
	}
	return nil
}

func (ct *CronTrigger) Init() error {
	var err error
	err = ct.GetLocation()
	if err != nil {
		return err
	}
	now := time.Now()
	if ct.StartTime == "" {
		ct.startTime = now.UTC().Unix()
		ct.startTimet = now.In(ct.timeZone)
		ct.StartTime = now.In(ct.timeZone).Format(time.DateTime)
	} else {
		sTime, err := time.ParseInLocation(time.DateTime, ct.StartTime, ct.timeZone)
		if err != nil {
			return fmt.Errorf(" StartTime `%s` TimeZone: %s error: %s", ct.StartTime, ct.TimeZoneName, err)
		}
		ct.startTime = sTime.UTC().Unix()
		ct.startTimet = sTime

	}

	if ct.EndTime == "" {
		ct.endTime = MaxDate.UTC().Unix()
	} else {
		eTime, err := time.ParseInLocation(time.DateTime, ct.EndTime, ct.timeZone)
		if err != nil {
			return fmt.Errorf(" EndTime `%s` TimeZone: %s error: %s", ct.EndTime, ct.TimeZoneName, err)
		}
		ct.endTime = eTime.Unix()

	}

	if ct.ExpireTime == 0 {
		ct.ExpireTime = 1
	}

	return nil
}

func (ct *CronTrigger) GetExpireTime() int64 {
	return ct.ExpireTime
}

func (ct CronTrigger) GetNextRunTime(previousFireTime, now int64) (int64, error) {
	expr, err := cronexpr.Parse(ct.CronExpr)
	if err != nil {
		return 0, fmt.Errorf(" CronExpr `%s` error: %s", ct.CronExpr, err)
	}
	var nextRunTime time.Time
	// 开始时间小于当前时间, 时间基点就是当前时间,反之就是开始时间
	if !ct.isInit {
		if err = ct.Init(); err != nil {
			return 0, err
		}
	}

	if now > ct.startTime {
		nextRunTime = expr.Next(time.Unix(int64(now), 0).In(ct.timeZone))
	} else {
		nextRunTime = expr.Next(ct.startTimet)
	}

	if ct.endTime < nextRunTime.Unix() {
		return 0, nil
	}

	return nextRunTime.UTC().Unix(), nil
}
