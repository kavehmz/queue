// Queue is a simple Queue system written in Go that will use Redis underneath.
// Focus of this design is mainly horisontal scalability via concurrency, paritioning and fault-detection
// Queues can be partitions in to more than one Redis and each redis can keep more than one queue.
//
// Number of redis paritions is set by using Partitions function and setting slice of Redis URL connections.
// To set number of queues in each redis you can use QueuesInPartision functions.
//
// Partitioning factor to identify where each event must save is the task ID which is set while using AddTask.
// RedisParition => Id % redisParitions
// Queue => (OrderID/redisParitions) % queuePartitions
//
// If more than two or more tasks are added with the same ID, this library will make sure they are all sent to the same analyzer.
// Using the same ID can help in cases that there is a relation between some tasks and they must be analyzed together.
package queue

import (
	"regexp"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

var queuePartitions = 1

var redisParitions = 1

type redisStruct struct {
	conn redis.Conn
	url  string
}

var redisPool []redisStruct

// QueuesInPartision set number of queue in each partition. Each analyser will work on one queue in one partition and start its workers.
// This paritioning is for safely distributing related task into one queue
func QueuesInPartision(n int) {
	queuePartitions = n
}

// Partitions will define the number of redis partitions for queue. It is useful if one redis for any reason cannot handle the load of analyser.
// This function will accept a slice of redisURL to set the redis pool.
func Partitions(urls []string) {
	redisParitions = len(urls)
	redisPool = redisPool[:0]
	for _, v := range urls {
		r, _ := redis.DialURL(v)
		redisPool = append(redisPool, redisStruct{r, v})
	}
}

// AddTask will add a task to the queue. It will accept an ID and an string (task).
// If more than one task are added with the same ID, queue will make sure they are send
// to the same analyser as long as analyers does not return before next ID is poped from the queue.
// Analyzer implementation is also a factor to in this case.
func AddTask(id int, task string) {
	task = strconv.Itoa(id) + ";" + task
	_, e := redisPool[id%redisParitions].conn.Do("RPUSH", "WAREHOUSE_"+strconv.Itoa((id/redisParitions)%queuePartitions), task)
	checkErr(e)
}

func waitforSuccess(n int, id int, success chan bool, pool map[int]chan string) {
	redisdb, _ := redis.DialURL(redisPool[id%redisParitions].url)
	redisdb.Do("SET", "PENDING::"+strconv.Itoa(id), 1)
	r := <-success
	if r {
		delete(pool, id)
		redisdb.Do("DEL", "PENDING::"+strconv.Itoa(id))
	}
}

func removeTask(redisdb redis.Conn, queue string) (int, string) {
	r, e := redisdb.Do("LPOP", queue)
	checkErr(e)
	if r != nil {
		s, _ := redis.String(r, e)
		m := regexp.MustCompile(`(\d+);(.*)$`).FindStringSubmatch(s)
		id, _ := strconv.Atoi(m[1])
		redisdb.Do("SET", "PENDING::"+strconv.Itoa(id), 1)
		return id, m[2]
	}
	return 0, ""
}

// AnalysePool will accept following paramters:
// n that is an int id which will specift which redis and which queue in that redis will be assign to this analyzer.
// poolSize which set max number of buffered Channels (concurrent workers) for analyzing the tasks.
// exitOnEmpy that shows if AnalysePool will exit as soon as queue is empty or it will wait for queue to refill for ever.
// analyzer is a function with following parameters (id int, msg_channel chan string, success chan bool, next chan bool)
// analyzer will get ID of the task it is working on in id,
// msg_channel will be used to send related tasks to the analyzer function. There can be more than one task with the same ID if needed.
// success to indicate success or failure of analyzer.
// next to signal when it is OK for AnalysePool to start the next tasks. Normally this should be calls before ending analyzer.
// If a task does not return by sucess or crashes, all non-complete analyzes can be found by searching for PENDING::* keys in Redis. * will indicate the ID of failed tasks.
func AnalysePool(n int, poolSize int, exitOnEmpy bool, analyzer func(int, chan string, chan bool, chan bool)) {
	redisdb := redisPool[n%redisParitions].conn
	queue := "WAREHOUSE_" + strconv.Itoa((n/redisParitions)%queuePartitions)
	next := make(chan bool, poolSize)
	pool := make(map[int]chan string)
	for {

		id, task := removeTask(redisdb, queue)

		if task == "" {
			if exitOnEmpy {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			if pool[id] == nil {
				pool[id] = make(chan string)
				success := make(chan bool)
				go analyzer(id, pool[id], success, next)
				go waitforSuccess(n, id, success, pool)
				pool[id] <- task
				next <- true
			} else {
				pool[id] <- task
			}
		}
	}

	for i := 0; i < poolSize; i++ {
		next <- true
	}
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}
