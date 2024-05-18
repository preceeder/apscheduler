//   File Name:  base.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import (
	"encoding/gob"
	"time"
)

var MaxDate = time.Date(9999, 12, 30, 23, 59, 59, 0, time.UTC)

const (
	TRIGGER_DATETIME = "datetime"
	TRIGGER_INTERVAL = "interval"
	TRIGGER_CRON     = "cron"
)

type Trigger interface {
	Init() error
	GetNextRunTime(previousFireTime, now time.Time) (time.Time, error)
}

func init() {
	gob.Register(&IntervalTrigger{})
	gob.Register(&DateTrigger{})
	gob.Register(&CronTrigger{})
	gob.Register(&time.Location{})

}
