//   File Name:  sqlx.go
//    Description: mysql
//    Author:      Chenghu
//    Date:       2024/5/17 18:18
//    Change Activity:

package stores

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/preceeder/apscheduler/apsError"
	"github.com/preceeder/apscheduler/job"
	"log/slog"
)

const TABLE_NAME = "go_jobs"

type MysqlConfig struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Password    string `json:"password"`
	User        string `json:"user"`
	Db          string `json:"db"`
	MaxOpenCons int    `json:"maxOpenCons"`
	MaxIdleCons int    `json:"MaxIdleCons"`
}

// Mysql table
type Jobs struct {
	ID          string  `db:"job_id"`
	NextTime    float64 `db:"next_run_time"`
	NextRunTime int64
	State       []byte `db:"state"`
}

// Stores jobs in a database table using sqlx.
// The table will be created if it doesn't exist in the database.
type MysqlStore struct {
	DB        *sqlx.DB
	TableName string
}

func NewMysqlStore(cf MysqlConfig, tableName string) *MysqlStore {
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", cf.User, cf.Password, cf.Host, cf.Port, cf.Db)
	slog.Info("链接数据库", "db", dsn)
	// 安全链接  内部已经ping 了
	db := sqlx.MustConnect("mysql", dsn)
	db.SetMaxOpenConns(cf.MaxOpenCons)
	db.SetMaxIdleConns(cf.MaxIdleCons)

	return &MysqlStore{
		DB:        db,
		TableName: tableName,
	}
}

func (s *MysqlStore) Init() error {
	if s.TableName == "" {
		s.TableName = TABLE_NAME
	}

	createTableSql := "CREATE TABLE IF NOT EXISTS `%s` (\n  `job_id` varchar(191) NOT NULL,\n  `next_run_time` double DEFAULT NULL,\n  `state` blob NOT NULL,\n  PRIMARY KEY (`job_id`),\n  KEY `ix_api_job_next_run_time` (`next_run_time`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	sql := fmt.Sprintf(createTableSql, s.TableName)
	_, err := s.DB.Exec(sql)
	if err != nil {
		return fmt.Errorf("failed to create table: %s", err)
	}

	return nil
}

func (s *MysqlStore) Close() error {
	return s.DB.Close()
}

func (s *MysqlStore) AddJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}

	//js := Jobs{ID: j.Id, NextRunTime: j.NextRunTime, State: state}
	insertSql := "insert into `%s` (job_id, next_run_time,state) values(:job_id, :next_run_time,:state)"

	params := map[string]any{
		"job_id":        j.Id,
		"next_run_time": j.NextRunTime,
		"state":         state,
	}

	_, err = s.DB.NamedExec(fmt.Sprintf(insertSql, s.TableName), params)
	return err
}

func (s *MysqlStore) GetJob(id string) (job.Job, error) {
	var js Jobs
	querySql := "select job_id, next_run_time, state from `%s` where job_id=:job_id"

	q, args := s.sqlPares(fmt.Sprintf(querySql, s.TableName), map[string]any{"job_id": id})
	err := s.DB.Get(&js, q, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return job.Job{}, apsError.JobNotFoundError(id)
		} else {
			return job.Job{}, err
		}
	}
	return s.StateLoad(js.State)
}

func (s *MysqlStore) GetAllJobs() ([]job.Job, error) {
	var jsList []*Jobs
	querySql := "select job_id, next_run_time, state from %s"

	q, args := s.sqlPares(fmt.Sprintf(querySql, s.TableName), nil)
	err := s.DB.Select(&jsList, q, args...)
	if err != nil {
		return nil, err
	}

	var jobList []job.Job
	for _, js := range jsList {
		aj, err := s.StateLoad(js.State)
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, aj)
	}

	return jobList, nil
}

func (s *MysqlStore) GetDueJobs(timestamp int64) ([]job.Job, error) {
	var jsList []*Jobs

	querySql := "select job_id, next_run_time, state from `%s` where next_run_time <= :next_run_time"

	q, args := s.sqlPares(fmt.Sprintf(querySql, s.TableName), map[string]any{"next_run_time": timestamp})
	err := s.DB.Select(&jsList, q, args...)
	if err != nil {
		return nil, err
	}

	var jobList []job.Job
	for _, js := range jsList {
		aj, err := s.StateLoad(js.State)
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, aj)
	}

	return jobList, nil
}

func (s *MysqlStore) UpdateJob(j job.Job) error {
	state, err := s.StateDump(j)
	if err != nil {
		return err
	}
	updateSql := "update `%s` set next_run_time=:next_run_time, state=:state where job_id=:job_id"
	params := map[string]any{
		"job_id":        j.Id,
		"next_run_time": j.NextRunTime,
		"state":         state,
	}
	_, err = s.DB.NamedExec(fmt.Sprintf(updateSql, s.TableName), params)

	return err
}

func (s *MysqlStore) DeleteJob(id string) error {
	deleteSql := "delete from `%s` where job_id=:job_id"
	_, err := s.DB.NamedExec(fmt.Sprintf(deleteSql, s.TableName), map[string]any{"job_id": id})
	return err
}

func (s *MysqlStore) DeleteAllJobs() error {
	deleteSql := "delete from `%s`"
	_, err := s.DB.NamedExec(fmt.Sprintf(deleteSql, s.TableName), nil)
	return err
}

func (s *MysqlStore) GetNextRunTime() (int64, error) {
	var nextRunTimeMini float64
	querySql := "select next_run_time from `%s` order by next_run_time asc limit 1"
	err := s.DB.Get(&nextRunTimeMini, fmt.Sprintf(querySql, s.TableName))

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		} else {
			return 0, err
		}
	}

	return int64(nextRunTimeMini), nil
}

func (s *MysqlStore) Clear() error {
	deleteSql := "drop `%s`"
	_, err := s.DB.NamedExec(fmt.Sprintf(deleteSql, s.TableName), nil)
	return err
}

func (s *MysqlStore) StateDump(j job.Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize Bytes and convert to Job
func (s *MysqlStore) StateLoad(state []byte) (job.Job, error) {
	var j job.Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return job.Job{}, err
	}
	return j, nil
}

// 参数解析
func (s *MysqlStore) sqlPares(osql string, params any) (sql string, args []any) {
	var err error
	sql, args, err = sqlx.Named(osql, params)
	if err != nil {
		slog.Error("sqlx.Named error :" + err.Error())
	}
	sql, args, err = sqlx.In(sql, args...)
	if err != nil {
		slog.Error("sqlx.In error :" + err.Error())
	}
	sql = s.DB.Rebind(sql)
	return sql, args
}
