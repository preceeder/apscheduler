//   File Name:  date.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import "time"

type DateTrigger struct {
	RunDate      time.Time `json:"run_date"`
	TimeZoneName string    `json:"time_zone_name"`
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
	return nil
}

func (dt *DateTrigger) GetNextRunTime(previousFireTime, now time.Time) (time.Time, error) {
	if !dt.isInit {
		if err := dt.Init(); err != nil {
			return time.Time{}, err
		}
	}
	if previousFireTime.IsZero() {
		if dt.RunDate.Before(now) {
			return time.Time{}, nil
		}
		return dt.RunDate.In(dt.timeZone), nil
	}
	return time.Time{}, nil
}
