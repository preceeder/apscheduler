//   File Name:  date.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import "time"

type DateTrigger struct {
	RunDate      string `json:"run_date"`
	TimeZoneName string `json:"time_zone_name"`
	ExpireTime   int64  `json:"expire_time"` // 任务超过多长时间就是过期, 过期则本次不执行 单位 time.Second, 也是误差值, 一般情况拿到这个任务做判定的时候 now > NextRunTime 比较微小的值
	runDate      int64
	timeZone     *time.Location
	isInit       bool
}

// GetLocation 获取时区
func (dt *DateTrigger) GetLocation() (err error) {
	dt.timeZone, err = time.LoadLocation(dt.TimeZoneName)
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

	if dt.ExpireTime == 0 {
		dt.ExpireTime = 1
	}
	return nil
}

func (dt *DateTrigger) GetExpireTime() int64 {
	return dt.ExpireTime
}

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
