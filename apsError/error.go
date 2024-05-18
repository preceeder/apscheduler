//   File Name:  error.go
//    Description:
//    Author:      Chenghu
//    Date:       2024/5/16 17:53
//    Change Activity:

package apsError

import (
	"fmt"
)

type JobNotFoundError string

func (e JobNotFoundError) Error() string {
	return fmt.Sprintf("jobId `%s` not found!", string(e))
}

var JobNotFoundErrorType JobNotFoundError

type FuncUnregisteredError string

func (e FuncUnregisteredError) Error() string {
	return fmt.Sprintf("function `%s` unregistered!", string(e))
}

type JobExpireError string

func (e JobExpireError) Error() string { return fmt.Sprintf("job is expire `%s`", string(e)) }

type JobTimeoutError struct {
	FullName string
	Timeout  string
	Err      error
}

func (e *JobTimeoutError) Error() string {
	return fmt.Sprintf("job `%s` Timeout `%s` error: %s!", e.FullName, e.Timeout, e.Err)
}
