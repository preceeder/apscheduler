//   File Name:  interval.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import (
	"fmt"
	"math"
	"time"
)

type IntervalTrigger struct {
	StartTime    string `json:"start_time"`     // 数据格式 time.DateTime  "2006-01-02 15:04:05"
	EndTime      string `json:"end_time"`       // 数据格式 time.DateTime  "2006-01-02 15:04:05"
	Interval     int64  `json:"interval"`       // 单位 ms
	TimeZoneName string `json:"time_zone_name"` // 默认UTC
	Jitter       int64  `json:"Jitter"`         // 时间误差, 超过这个误差时间就忽略本次执行, 单位 ms time.Millisecond
	timeZone     *time.Location
	startTime    int64

	endTime int64
	isInit  bool
}

// GetLocation 获取时区
func (it *IntervalTrigger) GetLocation() (err error) {
	it.timeZone, err = time.LoadLocation(it.TimeZoneName)
	if err != nil {
		return err
	}
	return nil
}

func (it *IntervalTrigger) Init() error {
	var err error
	err = it.GetLocation()
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	now := time.Now()
	if it.StartTime == "" {
		it.startTime = now.UTC().UnixMilli()
		it.StartTime = now.In(it.timeZone).Format(time.DateTime)
	} else {
		sTime, err := time.ParseInLocation(time.DateTime, it.StartTime, it.timeZone)
		if err != nil {
			return fmt.Errorf(" StartTime `%s` TimeZone: %s error: %s", it.StartTime, it.TimeZoneName, err)
		}
		it.startTime = sTime.UTC().UnixMilli()
	}

	if it.EndTime == "" {
		it.endTime = MaxDate.UTC().UnixMilli()
	} else {
		eTime, err := time.ParseInLocation(time.DateTime, it.EndTime, it.timeZone)
		if err != nil {
			return fmt.Errorf(" EndTime `%s` TimeZone: %s error: %s", it.EndTime, it.TimeZoneName, err)
		}
		it.endTime = eTime.UTC().UnixMilli()
	}
	it.isInit = true

	if it.Jitter == 0 {
		it.Jitter = 1000
	}

	return nil
}

func (it *IntervalTrigger) GetJitterTime() int64 {
	return it.Jitter
}

// GetNextRunTime
// previousFireTime   ms
// now   ms
func (it *IntervalTrigger) GetNextRunTime(previousFireTime, now int64) (int64, error) {
	var nextRunTime int64
	if !it.isInit {
		if err := it.Init(); err != nil {
			return 0, err
		}
	}
	if previousFireTime > 0 {
		if previousFireTime > now {
			return previousFireTime, nil
		}
		nextRunTime = previousFireTime + it.Interval
	} else if it.startTime > now {
		nextRunTime = it.startTime
	} else {
		timediffDuration := now - it.startTime
		nextIntervalNum := int64(math.Ceil(float64(timediffDuration / it.Interval)))
		nextRunTime = it.startTime + (it.Interval * nextIntervalNum)
	}

	if it.endTime < nextRunTime {
		return 0, nil
	}
	return nextRunTime, nil
}
