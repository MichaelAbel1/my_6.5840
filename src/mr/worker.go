package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 实现 sort.Interface 接口
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))

		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func generateMapResultFileName(id int, index int) string {
	return fmt.Sprintf("mr-%d-%d", id, index)
}

func writeFile(filePath string, buf *bytes.Buffer) {
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("atomicWriteFile: create file %v failed", filePath)
	}
	defer file.Close()
	_, err = file.Write(buf.Bytes())
}

func atomicWriteFile(filename string, r io.Reader) (err error) {
	// 先写入临时文件，再进行原子重命名
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	tmpf, err := os.CreateTemp(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	// 保证文件出错时被删除
	defer func() {
		if err != nil {
			os.Remove(tmpf.Name())
		}
	}()
	defer tmpf.Close()
	name := tmpf.Name()
	if _, err := io.Copy(tmpf, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := tmpf.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}

	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		log.Printf("doMapTask: file %v not exist", filename)
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't chmod tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("can't rename tempfile %q to %q: %v", name, filename, err)
	}

	return nil

}

// func atomicWriteFile(filename string, r io.Reader) (err error) {
// 	// write to a temp file first, then we'll atomically replace the target file
// 	// with the temp file.
// 	dir, file := filepath.Split(filename)
// 	if dir == "" {
// 		dir = "."
// 	}

// 	f, err := ioutil.TempFile(dir, file)
// 	if err != nil {
// 		return fmt.Errorf("cannot create temp file: %v", err)
// 	}
// 	defer func() {
// 		if err != nil {
// 			// Don't leave the temp file lying around on error.
// 			_ = os.Remove(f.Name()) // yes, ignore the error, not much we can do about it.
// 		}
// 	}()
// 	// ensure we always close f. Note that this does not conflict with  the
// 	// close below, as close is idempotent.
// 	defer f.Close()
// 	name := f.Name()
// 	if _, err := io.Copy(f, r); err != nil {
// 		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
// 	}
// 	if err := f.Close(); err != nil {
// 		return fmt.Errorf("can't close tempfile %q: %v", name, err)
// 	}

// 	// get the file mode from the original file and use that for the replacement
// 	// file, too.
// 	info, err := os.Stat(filename)
// 	if os.IsNotExist(err) {
// 		// no original file
// 		log.Printf("doMapTask: file %v not exist", filename)
// 	} else if err != nil {
// 		return err
// 	} else {
// 		if err := os.Chmod(name, info.Mode()); err != nil {
// 			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
// 		}
// 	}
// 	if err := os.Rename(name, filename); err != nil {
// 		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
// 	}
// 	return nil
// }

// 将map结果经过hash后写入文件
func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	filename := response.FilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMapTask: open file %v failed", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("doMapTask: read file %v failed", filename)
	}
	kva := mapF(filename, string(content))
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	errChan := make(chan error, 1) // 带缓冲的错误通道，确保有一个缓冲空间防止死锁
	var once sync.Once

	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			var buf bytes.Buffer              // 字节缓冲区，可以用于高效地将数据写入内存，比写入磁盘高效
			enc := json.NewEncoder(&buf)      // 创建一个新的 JSON 编码器 enc，该编码器将数据写入到前面定义的 buf 缓冲区中
			for _, kv := range intermediate { // 在内存中构建完整的 JSON 数据后一次性写入文件，可以确保文件内容的完整性和一致性
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			// 要么全部成功要么全部失败。这样可以避免部分写入导致的数据不一致问题。
			err := atomicWriteFile(intermediateFilePath, &buf) // 对于失败的任务不用处理(会超时重新运行)
			if err != nil {
				log.Printf("doMapTask: write intermediate file %v failed", intermediateFilePath)
				once.Do(func() {
					errChan <- err
				})
			}
		}(index, intermediate)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 等待错误或任务完成
	if err := <-errChan; err != nil {
		// 处理错误，可能是立即退出程序或执行其他逻辑
		log.Printf("%v : Map task %v failed: %v", filename, response.Id, err)
	} else {
		doReport(response.Id, MapPhase)
		log.Printf("%v : doMapTask: map task %v finished", filename, response.Id)
	}
}

func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		mapResultFileName := generateMapResultFileName(i, response.Id)
		file, err := os.Open(mapResultFileName)
		if err != nil {
			log.Fatalf("doReduceTask: open file %v failed", mapResultFileName)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	var buf bytes.Buffer
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reduceF(kva[i].Key, values)
		fmt.Fprintf(&buf, "%v %v\n", kva[i].Key, output)
		i = j
	}
	err := atomicWriteFile(generateReduceResultFileName(response.Id), &buf)
	if err != nil {
		log.Printf("doReduceTask id %v : write reduce result file failed", response.Id)
		return
	}
	doReport(response.Id, ReducePhase)
}

func generateReduceResultFileName(reduce_index int) string {
	return fmt.Sprintf("mr-out-%d", reduce_index)
}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
