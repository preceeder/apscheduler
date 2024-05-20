/*
File Name:  try.go
Description:
Author:      Chenghu
Date:       2023/8/25 11:39
Change Activity:
*/
package try

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

func CatchException(handle func(err any)) {
	if err := recover(); err != nil {
		handle(err)
	}
}

// RecordPrefix 需要 更具自己的项目, 加上项目的 module name;  不然捕获的错误信息里就不会包含自己项目的信息
// example: 自己项目的module 名是 match; 那么就需要  try.RecordPrefix = append(try.RecordPrefix, "apscheduler")
var RecordPrefix = []string{"apscheduler"}

// PrintStackTrace 打印全部堆栈信息
func PrintStackTrace(err any) string {
	buf := new(bytes.Buffer)
	if err != nil {
		fmt.Fprintf(buf, "%v \n ", err)
	}
	for i := 1; true; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			fmt.Fprintf(buf, "%s:%d \n ", file, line)
			break
		} else {
			prevFunc := runtime.FuncForPC(pc).Name()
			for _, prefix := range RecordPrefix {
				if strings.HasPrefix(prevFunc, prefix) {
					fmt.Fprintf(buf, "%s:%d \n ", file, line)
					continue
				}
			}
		}
	}
	return buf.String()
}

// GetStackTrace 打印堆栈信息 指定调用栈的上一级信息
// funcName 函数名, step 从funcNmae开始记录多少层
func GetStackTrace(funcName string, step int) string {
	buf := new(bytes.Buffer)
	start := false
	for i := 1; true; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			fmt.Fprintf(buf, "%s:%d", file, line)
			break
		} else {
			prevFunc := runtime.FuncForPC(pc).Name()
			if !start {
				if strings.HasSuffix(prevFunc, funcName) {
					start = true
				}
				continue
			}

			for _, prefix := range RecordPrefix {
				if strings.HasPrefix(prevFunc, prefix) {
					if step > 0 {
						fmt.Fprintf(buf, "%s:%d \n ", file, line)
						step -= 1
						continue
					}
					break
				}
			}
		}
	}
	return buf.String()
}
