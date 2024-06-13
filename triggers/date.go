//   File Name:  date.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import "time"

type DateTrigger struct {
	RunDate      string `json:"run_date"`      // 数据格式 time.DateTime "2006-01-02 15:04:05"
	TimeZoneName string `json:"utc_time_zone"` // 默认UTC
	Jitter       int64  `json:"Jitter"`        // 时间误差, 超过这个误差时间就忽略本次执行,默认 0 表示不管误差, 单位 s time.Second
	runDate      int64
	timeZone     *time.Location
	isInit       bool
}

// GetLocation 获取时区
func (dt *DateTrigger) GetLocation() (err error) {
	if dt.TimeZoneName == "" {
		dt.TimeZoneName = DefaultTimeZone
	}
	dt.timeZone, err = ParseUtcTimeOffset(dt.TimeZoneName)
	//dt.timeZone, err = time.LoadLocation(dt.TimeZoneName)
	if err != nil {
		return err
	}
	return nil
}
func (dt *DateTrigger) Init() error {
	err := dt.GetLocation()
	if err != nil {
		return err
	}

	rt, err := time.ParseInLocation(dt.RunDate, time.DateTime, dt.timeZone)
	if err != nil {
		return err
	}

	dt.runDate = rt.UTC().Unix()

	//if dt.Jitter == 0 {
	//	dt.Jitter = 1000
	//}
	return nil
}

func (dt *DateTrigger) GetJitterTime() int64 {
	return dt.Jitter
}

// GetNextRunTime
// previousFireTime   s
// now   s
func (dt *DateTrigger) GetNextRunTime(previousFireTime, now int64) (int64, error) {
	if !dt.isInit {
		if err := dt.Init(); err != nil {
			return 0, err
		}
	}
	if previousFireTime == 0 {
		if dt.runDate <= now {
			return 0, nil
		}
		return dt.runDate, nil
	}
	return 0, nil
}
