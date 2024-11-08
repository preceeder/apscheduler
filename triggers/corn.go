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

// CronTrigger
// CronExpr
// Field name     Mandatory?   Allowed values    Allowed special characters
// ----------     ----------   --------------    --------------------------
// Seconds        No           0-59              * / , -
// Minutes        Yes          0-59              * / , -
// Hours          Yes          0-23              * / , -
// Day of month   Yes          1-31              * / , - L W
// Month          Yes          1-12 or JAN-DEC   * / , -
// Day of week    Yes          0-6 or SUN-SAT    * / , - L #
// Year           No           1970–2099         * / , -

// Asterisk ( * )
// The asterisk indicates that the cron expression matches for all values of the field. E.g., using an asterisk in the 4th field (month) indicates every month.
//
// Slash ( / )
// Slashes describe increments of ranges. For example 3-59/15 in the minute field indicate the third minute of the hour and every 15 minutes thereafter. The form */... is equivalent to the form "first-last/...", that is, an increment over the largest possible range of the field.
//
// Comma ( , )
// Commas are used to separate items of a list. For example, using MON,WED,FRI in the 5th field (day of week) means Mondays, Wednesdays and Fridays.
//
// Hyphen ( - )
// Hyphens define ranges. For example, 2000-2010 indicates every year between 2000 and 2010 AD, inclusive.
//
// L
// L stands for "last". When used in the day-of-week field, it allows you to specify constructs such as "the last Friday" (5L) of a given month. In the day-of-month field, it specifies the last day of the month.
//
// W
// The W character is allowed for the day-of-month field. This character is used to specify the business day (Monday-Friday) nearest the given day. As an example, if you were to specify 15W as the value for the day-of-month field, the meaning is: "the nearest business day to the 15th of the month."
//
// So, if the 15th is a Saturday, the trigger fires on Friday the 14th. If the 15th is a Sunday, the trigger fires on Monday the 16th. If the 15th is a Tuesday, then it fires on Tuesday the 15th. However if you specify 1W as the value for day-of-month, and the 1st is a Saturday, the trigger fires on Monday the 3rd, as it does not 'jump' over the boundary of a month's days.
//
// The W character can be specified only when the day-of-month is a single day, not a range or list of days.
//
// The W character can also be combined with L, i.e. LW to mean "the last business day of the month."
//
// Hash ( # )
// # is allowed for the day-of-week field, and must be followed by a number between one and five. It allows you to specify constructs such as "the second Friday" of a given month.

//If only six fields are present, a 0 second field is prepended, that is, * * * * * 2013 internally become 0 * * * * * 2013.
//If only five fields are present, a 0 second field is prepended and a wildcard year field is appended, that is, * * * * Mon internally become 0 * * * * Mon *.
//Domain for day-of-week field is [0-7] instead of [0-6], 7 being Sunday (like 0). This to comply with http://linux.die.net/man/5/crontab#.
//As of now, the behavior of the code is undetermined if a malformed cron expression is supplied
type CronTrigger struct {
	CronExpr     string `json:"cron_expr"`
	TimeZoneName string `json:"utc_time_zone"` // 默认就是 UTC
	StartTime    string `json:"start_time"`    // 数据格式 time.DateTime "2006-01-02 15:04:05"
	EndTime      string `json:"end_time"`      // 数据格式 time.DateTime "2006-01-02 15:04:05"
	Jitter       int64  `json:"Jitter"`        // 时间误差, 超过这个误差时间就忽略本次执行, 默认 0 表示不管误差, 单位 s time.Second,

	startTime  int64
	startTimet time.Time
	endTime    int64
	timeZone   *time.Location
	isInit     bool
}

// GetLocation 获取时区
func (ct *CronTrigger) GetLocation() (err error) {
	if ct.TimeZoneName == "" {
		ct.TimeZoneName = DefaultTimeZone
	}
	ct.timeZone, err = ParseUtcTimeOffset(ct.TimeZoneName)
	//ct.timeZone, err = time.LoadLocation(ct.TimeZoneName)
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
	}

	if ct.EndTime == "" {
		ct.endTime = MaxDate.UTC().Unix()
	} else {
		eTime, err := time.ParseInLocation(time.DateTime, ct.EndTime, ct.timeZone)
		if err != nil {
			return fmt.Errorf(" EndTime `%s` TimeZone: %s error: %s", ct.EndTime, ct.TimeZoneName, err)
		}
		ct.endTime = eTime.UTC().Unix()
	}
	//if dt.Jitter == 0 {
	//	dt.Jitter = 1000
	//}

	return nil
}

func (ct *CronTrigger) GetJitterTime() int64 {
	return ct.Jitter
}

// GetNextRunTime
// previousFireTime   s
// now   s
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
		nextRunTime = expr.Next(time.Unix(now, 0).In(ct.timeZone))
	} else {
		nextRunTime = expr.Next(ct.startTimet)
	}

	if ct.endTime < nextRunTime.Unix() {
		return 0, nil
	}

	return nextRunTime.UTC().Unix(), nil
}
