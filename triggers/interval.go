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
	StartTime    string `json:"start_time"` // time.DateTime
	EndTime      string `json:"end_time"`   // time.DateTime
	Interval     string `json:"interval"`
	TimeZoneName string `json:"time_zone_name"`
	timeZone     *time.Location
	startTime    time.Time
	endTime      time.Time
	interval     time.Duration
	isInit       bool
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

	it.interval, err = it.GetInterval()
	if err != nil {
		return err
	}

	if it.StartTime == "" {
		it.startTime = time.Now().UTC()
	} else {
		it.startTime, err = time.ParseInLocation(time.DateTime, it.StartTime, it.timeZone)
		if err != nil {
			return fmt.Errorf(" StartTime `%s` TimeZone: %s error: %s", it.StartTime, it.TimeZoneName, err)
		}
	}

	if it.EndTime == "" {
		it.endTime = MaxDate.UTC()
	} else {
		it.endTime, err = time.ParseInLocation(time.DateTime, it.EndTime, it.timeZone)
		if err != nil {
			return fmt.Errorf(" EndTime `%s` TimeZone: %s error: %s", it.EndTime, it.TimeZoneName, err)
		}
	}
	it.isInit = true

	return nil
}

func (it *IntervalTrigger) GetInterval() (time.Duration, error) {
	i, err := time.ParseDuration(it.Interval)
	if err != nil {
		return 0, fmt.Errorf("Interval `%s` error: %s", it.Interval, err)
	}
	return i, nil
}

func (it *IntervalTrigger) GetNextRunTime(previousFireTime, now time.Time) (time.Time, error) {
	var nextRunTime time.Time
	if !it.isInit {
		if err := it.Init(); err != nil {
			return time.Time{}, err
		}
	}
	if !previousFireTime.IsZero() {
		if !previousFireTime.Before(now) {
			return previousFireTime, nil
		}
		nextRunTime = previousFireTime.Add(it.interval)
	} else if it.startTime.Sub(now) > 0 {
		nextRunTime = it.startTime
	} else {
		timediffDuration := now.Sub(it.startTime)
		nextIntervalNum := time.Duration(math.Ceil(float64(timediffDuration / it.interval)))
		nextRunTime = it.startTime.Add(it.interval * nextIntervalNum)
	}
	if it.endTime.Before(nextRunTime) {
		return time.Time{}, nil
	}
	return nextRunTime, nil
}
