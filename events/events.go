//   File Name:  events.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/20 13:38
//    Change Activity:

package events

import (
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/stores"
	"github.com/preceeder/apscheduler/try"
	"log/slog"
)

type EventInfo struct {
	EventCode Event
	Job       *job.Job
	Store     *stores.Store
	Error     error
	Msg       string
}

var EventChan chan EventInfo = make(chan EventInfo, 100)

type Event int

const (
	EVENT_JOBSTORE_ADDED   Event = 1 << 0
	EVENT_JOBSTORE_REMOVED Event = 1 << iota
	EVENT_ALL_JOBS_REMOVED Event = 1 << iota
	EVENT_JOB_ADDED        Event = 1 << iota
	EVENT_JOB_REMOVED      Event = 1 << iota
	EVENT_JOB_MODIFIED     Event = 1 << iota
	EVENT_JOB_EXECUTED     Event = 1 << iota
	EVENT_JOB_ERROR        Event = 1 << iota
	EVENT_JOB_MISSED       Event = 1 << iota
)

type EventFunc func(ei EventInfo)

var EventMap = make(map[Event]EventFunc, 0)

func RegisterEvent(eventType Event, ef EventFunc) {
	EventMap[eventType] = ef
}

// StartEventsListen 开启事物监听
func StartEventsListen() {
	go func() {
		defer try.CatchException(func(err any) {
			slog.Error("EventsHandler error", "error", err)
		})
		for {
			select {
			case ch := <-EventChan:
				for code, fn := range EventMap {
					if (code & ch.EventCode) == ch.EventCode {
						fn(ch)
					}
				}
			}
		}
	}()

}
