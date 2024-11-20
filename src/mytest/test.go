package main

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	return len(key) % 5 // 示例哈希函数
}

// func main() {
// 	response := struct {
// 		NReduce int
// 	}{
// 		NReduce: 5,
// 	}

// 	intermediates := make([][]KeyValue, response.NReduce)
// 	intermediates[4] = make([]KeyValue, 0, 5)

// 	kv := KeyValue{Key: "key1", Value: "value1"}

// 	// 第一种方式
// 	intermediates[ihash(kv.Key)%response.NReduce] = append(intermediates[ihash(kv.Key)%response.NReduce], kv)
// 	fmt.Println("First way:", intermediates)

// 	// 第二种方式
// 	_ = append(intermediates[ihash(kv.Key)%response.NReduce], kv)
// 	fmt.Println("Second way:", intermediates)
// }
