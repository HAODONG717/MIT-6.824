package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}

type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

type Coordinator struct {
	// Your definitions here.
	JobChannelMap        chan *Job
	JobChannelReduce     chan *Job
	ReducerNum           int
	MapNum               int
	CoordinatorCondition Condition
	uniqueJobId          int
	jobMetaHolder        JobMetaHolder
	mu                   sync.Mutex
}

type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReducerNum int
}

func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	jobId := JobInfo.JobPtr.JobId
	if _, ok := j.MetaMap[jobId]; ok {
		fmt.Printf("meta contains job which id = %v", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
}

func (j *JobMetaHolder) getJobMetaInfo(JobId int) (*JobMetaInfo, bool) {
	if jobInfo, ok := j.MetaMap[JobId]; ok {
		return jobInfo, true
	} else {
		return nil, false
	}
}

func (j *JobMetaHolder) sendJob(JobId int) bool {
	jobInfo, ok := j.getJobMetaInfo(JobId)
	if !ok || jobInfo.condition != JobWaiting {
		return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true
}

func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	return (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0)
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		c.CoordinatorCondition = AllDone
	}
}

func (c *Coordinator) makeMapJobs(files []string) {
	for _, val := range files {
		id := c.generateJobId()
		job := &Job{
			JobType:    MapJob,
			InputFile:  []string{val},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}

		jobMetaInfo := &JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    job,
		}
		c.jobMetaHolder.putJob(jobMetaInfo)
		fmt.Println("put the job and job id = : ", job.JobId)
		c.JobChannelMap <- job
	}
	fmt.Println("making map jobs has done")
	c.jobMetaHolder.checkJobDone()

}

func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		fmt.Println("making reduce job :", id)
		JobToDo := Job{
			JobType:   ReduceJob,
			JobId:     id,
			InputFile: TmpFileAssignHelper(i, "main/mr-tmp"),
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &JobToDo,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		c.JobChannelReduce <- &JobToDo

	}
	//defer close(c.JobChannelReduce)
	fmt.Println("done making reduce jobs")
	c.jobMetaHolder.checkJobDone()
}

func (c *Coordinator) generateJobId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
}

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("coodinator get a request from worker : ")
	if c.CoordinatorCondition == MapPhase {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.sendJob(reply.JobId) {
				fmt.Printf("[duplicated job id = %d] is runnnig", reply.JobId)
			}
		} else {
			reply.JobType = WaitingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			if !c.jobMetaHolder.sendJob(reply.JobId) {
				fmt.Printf("job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaitingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
		}
		return nil
	} else {
		reply.JobType = KillJob
	}
	return nil
}

func (c *Coordinator) JobIsDone(job *Job, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch job.JobType {
	case MapJob:
		if jobInfo, ok := c.jobMetaHolder.getJobMetaInfo(job.JobId); ok && jobInfo.condition == JobWorking {
			jobInfo.condition = JobDone
			fmt.Printf("Map task on %d complete\n", job.JobId)
		} else {
			fmt.Printf("[duplicated job done job id = %d]", job.JobId)
		}
		break
	case ReduceJob:
		if jobInfo, ok := c.jobMetaHolder.getJobMetaInfo(job.JobId); ok && jobInfo.condition == JobWorking {
			jobInfo.condition = JobDone
			fmt.Printf("Map task on %d complete\n", job.JobId)
		} else {
			fmt.Printf("[duplicated job done job id = %d]", job.JobId)
		}
		break
	default:
		panic("wrong job done")
	}
	return nil
}

func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

// Your code here -- RPC handlers for the worker to call.

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
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		CoordinatorCondition: MapPhase,
		ReducerNum:           nReduce,
		MapNum:               len(files),
		uniqueJobId:          0,
	}

	c.makeMapJobs(files)

	c.server()

	go c.CrashHandler()

	return &c
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		c.mu.Lock()
		if c.CoordinatorCondition == AllDone {
			c.mu.Unlock()
			return
		}

		timenow := time.Now()
		for _, v := range c.jobMetaHolder.MetaMap {
			if v.condition == JobWorking {
				fmt.Println("job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
			}
			if v.condition == JobWorking && time.Now().Sub(v.StartTime) > 10*time.Second {
				fmt.Println("detect a crash on job ", v.JobPtr.JobId)
				switch v.JobPtr.JobType {
				case MapJob:
					c.JobChannelMap <- v.JobPtr
					v.condition = JobWaiting
				case ReduceJob:
					c.JobChannelReduce <- v.JobPtr
					v.condition = JobWaiting
				}
			}
		}
		c.mu.Unlock()
	}

}
