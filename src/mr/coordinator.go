package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finished
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type SchedulePhase uint8

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

func (phase SchedulePhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	nMap        int
	phase       SchedulePhase
	tasks       []Task
	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{} // 调用完成后，通知worker退出
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{
		response,
		make(chan struct{}),
	}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil

}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{
		request,
		make(chan struct{}),
	}
	c.reportCh <- msg
	<-msg.ok

	return nil
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, c.nMap)
	// c.nMap == len(c.files))
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName: file,
			id:       index,
			status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewJob := true, false
	for index, task := range c.tasks {
		switch task.status {
		case Idle:
			allFinished, hasNewJob = false, true
			c.tasks[index].startTime, c.tasks[index].status = time.Now(), Working
			response.NReduce, response.Id = c.nReduce, index
			if c.phase == MapPhase {
				response.JobType, response.FilePath = MapJob, c.files[index]
			} else {
				response.JobType, response.NMap = ReduceJob, c.nMap
			}
		case Working:
			allFinished = false
			if time.Since(task.startTime) > MaxTaskRunInterval { // 任务超时，则重新运行
				hasNewJob = true
				c.tasks[index].startTime, c.tasks[index].status = time.Now(), Working
				response.NReduce, response.Id = c.nReduce, index
				if c.phase == MapPhase {
					response.JobType, response.FilePath = MapJob, c.files[index]
				} else {
					response.JobType, response.NMap = ReduceJob, c.nMap
				}

			}
		case Finished:

		}
		if hasNewJob {
			break
		}
	}

	if !hasNewJob {
		response.JobType = WaitJob
	}

	return allFinished
}
func (c *Coordinator) schedule() {
	c.initMapPhase()

	for { // 可以替换为两个独立的 for range 循环，分别处理两个通道。这将使代码更清晰，每个循环只关注一个通道的消息。（其中一个用协程处理）
		select {
		case msg := <-c.heartbeatCh:
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) { // 当前阶段任务都已经完成，未完成则等待
				switch c.phase {
				case MapPhase:
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response) // 如果心跳消息快 则可不加？
				case ReducePhase:
					log.Printf("Coordinator: %v finished, All jobs finished! \n", ReducePhase)
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				case CompletePhase:
					panic(fmt.Sprintf("Coordinator: enter unexpected branch"))
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{} // 心跳消息完成
		case msg := <-c.reportCh:
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{} // 报告消息完成
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == CompletePhase {
		ret = true
	}
	// <-c.doneCh
	// ret = true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}
	c.server()
	go c.schedule()

	return &c
}
