//   File Name:  base.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 11:46
//    Change Activity:

package triggers

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"
)

// MaxDate 不能太大,  time.Duration 最大只能表示290年   默认值启动时间到当前时间距离100年
var MaxDate = time.Now().Add(time.Hour * 24 * 365 * 100).UTC()

// DefaultTimeZone 世界时区共有 26个 UTC-12 ~ UTC+14
// DefaultTimeZone UTC+9:30
var DefaultTimeZone = "UTC"

type Trigger interface {
	Init() error
	GetLocation() error
	GetJitterTime() int64
	GetNextRunTime(previousFireTime, now int64) (int64, error)
	ToMap() map[string]any
}

func init() {

}

var timeZoneMap = make(map[string]*time.Location)

// ParseUtcTimeOffset
// TUC pares   "UTC+12"  -> 12*60*60 -> time.FixedZone("UTC+12", 12*60*60)
func ParseUtcTimeOffset(offsetStr string) (location *time.Location, err error) {
	if lz, ok := timeZoneMap[offsetStr]; ok {
		return lz, nil
	}
	mcp := regexp.MustCompile(`UTC$|UTC([-+])(\d+)$|UTC([-+])(\d+):(\d+)$`)
	fin := mcp.FindStringSubmatch(offsetStr)
	offset := 0
	sign := 1
	if len(fin) > 0 {
		validData := slices.DeleteFunc(fin, func(e string) bool {
			if e == "" {
				return true
			}
			return false
		})
		for index, d := range validData {
			if d == offsetStr {
				continue
			}
			switch index {
			case 1:
				if d == "-" {
					sign = -1
				}
			case 2:
				hour, err := strconv.Atoi(d)
				if err != nil {
					return nil, err
				}
				offset += hour * 60 * 60
			case 3:
				minute, err := strconv.Atoi(d)
				if err != nil {
					return nil, err
				}
				offset += minute * 60
			}
		}
		offset *= sign
		location = time.FixedZone(offsetStr, offset)
		timeZoneMap[offsetStr] = location
		return
	}

	return nil, errors.New(fmt.Sprintf("TimeZoneName `%s` is not UTC", offsetStr))
}
