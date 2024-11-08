package serialize

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	jsonvalue "github.com/Andrew-M-C/go.jsonvalue"
	"github.com/preceeder/apscheduler/job"
	"github.com/preceeder/apscheduler/triggers"
	"reflect"
	"time"
)

func init() {
	gob.Register(&triggers.IntervalTrigger{})
	gob.Register(&triggers.DateTrigger{})
	gob.Register(&triggers.CronTrigger{})
	gob.Register(&time.Location{})
}

// Serialize
type Serialize interface {
	StateDump(job.Job) ([]byte, error)
	StateLoad([]byte) (job.Job, error)
}

// GobSerialize
type GobSerialize struct {
}

func (s *GobSerialize) StateDump(j job.Job) ([]byte, error) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize Bytes and convert to Job
func (s *GobSerialize) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return job.Job{}, err
	}

	return j, nil
}

type JsonSerialize struct {
}

var TriggerNameMap = map[string]reflect.Type{
	reflect.TypeOf(triggers.CronTrigger{}).Name():     reflect.TypeOf(triggers.CronTrigger{}),
	reflect.TypeOf(triggers.DateTrigger{}).Name():     reflect.TypeOf(triggers.DateTrigger{}),
	reflect.TypeOf(triggers.IntervalTrigger{}).Name(): reflect.TypeOf(triggers.IntervalTrigger{}),
}

func (s *JsonSerialize) StateDump(j job.Job) ([]byte, error) {
	marshal, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

// Deserialize Bytes and convert to Job
func (s *JsonSerialize) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	value, err := jsonvalue.Unmarshal(state)
	// 先单独拿到trigger数据, 序列化， 然后删除
	trigger, err := value.Get("trigger")
	if err != nil {
		return job.Job{}, err
	}
	triggerStr, _ := trigger.Marshal()
	err = value.Delete("trigger")
	if err != nil {
		return job.Job{}, err
	}

	// 将其他数据序列化
	jobOtherData, err := value.Marshal()
	if err != nil {
		return job.Job{}, err
	}
	// 将其他数据反序列化
	err = json.Unmarshal(jobOtherData, &j)
	if err != nil {
		return job.Job{}, err
	}
	// 还原Trigger
	tri := reflect.New(TriggerNameMap[j.TriggerName]).Interface()
	err = json.Unmarshal(triggerStr, tri)
	if err != nil {
		return job.Job{}, err
	}
	j.Trigger = tri.(triggers.Trigger)

	return j, nil
}

var DefaultSerialize Serialize = &GobSerialize{}

func SetDefaultSerialize(s Serialize) {
	DefaultSerialize = s
}
