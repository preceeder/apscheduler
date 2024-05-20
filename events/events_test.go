//   File Name:  events_test.go.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/20 14:15
//    Change Activity:

package events

import (
	"context"
	"fmt"
	"github.com/preceeder/apscheduler/job"
	"testing"
)

func TestRegisterEvent(t *testing.T) {
	type args struct {
		eventType Event
		ef        EventFunc
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{name: "o1", args: args{eventType: EVENT_JOB_EXECUTED | EVENT_JOB_ADDED, ef: func(ctx context.Context, e Event, jb job.Job, msg string) { fmt.Println("hahah", e, msg) }}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterEvent(tt.args.eventType, tt.args.ef)
			EventsHandler(context.Background(), EVENT_JOB_EXECUTED, job.Job{}, "不是吧,还要这样")

		})
	}
}
