package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

func (job JobType) String() string {
	switch job {
	case MapJob:
		return "MapJob"
	case ReduceJob:
		return "ReduceJob"
	case WaitJob:
		return "WaitJob"
	case CompleteJob:
		return "CompleteJob"
	}
	panic(fmt.Sprintf("unexpected jobType %d", job))
}

type HeartbeatRequest struct {
}

type HeartbeatResponse struct {
	FilePath string
	JobType  JobType
	NReduce  int
	NMap     int
	Id       int // response.Id表示有任务的id
}

func (response HeartbeatResponse) String() string {
	switch response.JobType {
	case MapJob:
		return fmt.Sprintf("{JobType:%v, FilePath:%v, Id:%v, NReduce:%v}", response.JobType, response.FilePath, response.Id, response.NReduce)
	case ReduceJob:
		return fmt.Sprintf("{JobType:%v, Id:%v, NMap:%v, NReduce:%v}", response.JobType, response.Id, response.NMap, response.NReduce)
	case WaitJob, CompleteJob:
		return fmt.Sprintf("{JobType:%v}", response.JobType)
	}
	panic(fmt.Sprintf("unexpected JobType %d", response.JobType))
}

type ReportRequest struct {
	Id    int
	Phase SchedulePhase
}

func (request ReportRequest) String() string {
	return fmt.Sprintf("{Id:%v, SchedulePhase:%v}", request.Id, request.Phase)
}

type ReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
