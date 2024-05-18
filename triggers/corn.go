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
	startTime    time.Time
	endTime      time.Time
	timeZone     *time.Location
	isInit       bool
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
	if ct.StartTime == "" {
		ct.startTime = time.Now().UTC()
	} else {
		ct.startTime, err = time.ParseInLocation(time.DateTime, ct.StartTime, ct.timeZone)
		if err != nil {
			return fmt.Errorf(" StartTime `%s` TimeZone: %s error: %s", ct.StartTime, ct.TimeZoneName, err)
		}
	}

	if ct.EndTime == "" {
		ct.endTime = MaxDate.UTC()
	} else {
		ct.endTime, err = time.ParseInLocation(time.DateTime, ct.EndTime, ct.timeZone)
		if err != nil {
			return fmt.Errorf(" EndTime `%s` TimeZone: %s error: %s", ct.EndTime, ct.TimeZoneName, err)
		}
	}

	return nil
}

func (ct CronTrigger) GetNextRunTime(previousFireTime, now time.Time) (time.Time, error) {
	expr, err := cronexpr.Parse(ct.CronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf(" CronExpr `%s` error: %s", ct.CronExpr, err)
	}
	var nextRunTime time.Time
	// 开始时间小于当前时间, 时间基点就是当前时间,反之就是开始时间
	if !ct.isInit {
		if err = ct.Init(); err != nil {
			return time.Time{}, err
		}
	}
	if now.After(ct.startTime) {
		nextRunTime = expr.Next(now.In(ct.timeZone))
	} else {
		nextRunTime = expr.Next(ct.startTime)
	}

	if ct.endTime.Before(nextRunTime) {
		return time.Time{}, nil
	}

	return nextRunTime, nil
}
