//   File Name:  base_test.go.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/20 16:45
//    Change Activity:

package triggers

import (
	"testing"
	"time"
)

func TestParseUTCTimeOffset(t *testing.T) {
	type args struct {
		offsetStr string
	}
	tests := []struct {
		name    string
		args    args
		want    *time.Location
		wantErr bool
	}{
		// TODO: Add test cases.
		{name: "test", args: args{offsetStr: "UTC+6:34"}, want: time.FixedZone("UTC+6:34", 6*3600+34*60)},
		{name: "test", args: args{offsetStr: "UTC+6"}, want: time.FixedZone("UTC+6", 6*3600)},
		{name: "test", args: args{offsetStr: "UTC"}, want: time.FixedZone("UTC", 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUtcTimeOffset(tt.args.offsetStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUTCTimeOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.String() != tt.want.String() {
				t.Errorf("ParseUTCTimeOffset() got = %v, want %v", got, tt.want)
			}
		})
	}
}
