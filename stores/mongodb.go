package stores

// 还没有测试, 有问题, 等需要用到的时候在测试
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/job"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var DATABASE = "apscheduler"
var COLLECTION = "jobs"

// Stores jobs in a MongoDB database.
type MongoDBStore struct {
	Client     *mongo.Client
	Database   string
	Collection string
	coll       *mongo.Collection
}

func (s *MongoDBStore) Init() error {
	if s.Database == "" {
		s.Database = DATABASE
	}
	if s.Collection == "" {
		s.Collection = COLLECTION
	}

	s.coll = s.Client.Database(s.Database).Collection(s.Collection)

	indexModel := mongo.IndexModel{
		Keys: bson.M{
			"next_run_time": 1,
		},
	}
	_, err := s.coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create index: %s", err)
	}

	return nil
}

func (s *MongoDBStore) Close() error {
	return s.Client.Disconnect(ctx)
}

func (s *MongoDBStore) AddJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}

	_, err = s.coll.InsertOne(ctx,
		bson.M{
			"_id":           j.Id,
			"next_run_time": j.NextRunTime,
			"state":         state,
		},
	)

	return err
}

func (s *MongoDBStore) GetJob(id string) (job.Job, error) {
	var result bson.M
	err := s.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return job.Job{}, apsError.JobNotFoundError(id)
	}
	if err != nil {
		return job.Job{}, err
	}

	state := result["state"].(primitive.Binary).Data
	return s.StateLoad(state)
}

func (s *MongoDBStore) GetAllJobs() ([]job.Job, error) {
	cursor, err := s.coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var jobList []job.Job
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		state := result["state"].(primitive.Binary).Data
		aj, err := s.StateLoad(state)
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, aj)
	}

	return jobList, nil
}

func (s *MongoDBStore) GetDueJobs(timestamp int64) ([]job.Job, error) {
	var jobList []job.Job

	filter := bson.M{"next_run_time": bson.M{"$lte": timestamp}}
	cursor, err := s.coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}

	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		state := result["state"].(primitive.Binary).Data
		aj, err := s.StateLoad(state)
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, aj)
	}

	return jobList, nil
}

func (s *MongoDBStore) UpdateJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}

	var result bson.M
	err = s.coll.FindOneAndReplace(ctx,
		bson.M{"_id": j.Id},
		bson.M{
			"next_run_time": j.NextRunTime,
			"state":         state,
		},
	).Decode(&result)

	return err
}

func (s *MongoDBStore) DeleteJob(id string) error {
	_, err := s.coll.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (s *MongoDBStore) DeleteAllJobs() error {
	_, err := s.coll.DeleteMany(ctx, bson.M{})
	return err
}

func (s *MongoDBStore) GetNextRunTime() (int64, error) {
	var result bson.M
	opts := options.FindOne().SetSort(bson.M{"next_run_time": 1})
	err := s.coll.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		return 0, err
	}
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}

	nextRunTimeMin := result["next_run_time"].(int64)
	return nextRunTimeMin, nil
}

func (s *MongoDBStore) Clear() error {
	return s.Client.Database(s.Database).Collection(s.Collection).Drop(ctx)
}

func (s *MongoDBStore) StateDump(j job.Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize Bytes and convert to Job
func (s *MongoDBStore) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}
