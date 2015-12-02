// queue is a simple Queue system written in Go that will use Redis underneath.
// Focus of this design is mainly horisontal scalability via concurrency, paritioning and fault-detection
// Queues can be partitions in to more than one Redis and each redis can keep more than one queue.
//
// Number of redis paritions is set by using Partitions function and setting slice of Redis URL connections.
// To set number of queues in each redis you can use QueuesInPartition functions.
//
// Partitioning factor to identify where each event must save is the task ID which is set while using AddTask.
// RedisParition => Id % redisParitions.
//
// Queue => (OrderID/redisParitions) % queuePartitions.
//
// If more than two or more tasks are added with the same ID, this library will make sure they are all sent to the same analyzer.
// Using the same ID can help in cases that there is a relation between some tasks and they must be analyzed together.
package queue

import (
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

//Queue defines a queue and its config.
type Queue struct {
	// AnalyzeBuff will set number of concurrent running anlyzers. It will default to number of cpu if not set.
	AnalyzerBuff int
	// QueueName this will set the name used in udnerlying system for the queue. Default is "QUEUE"
	QueueName string
	paritions int
	urls      []string
	pool      []redis.Conn
}

// Urls will accept a slice of redis connection URLS. This slice will setup the connections and also set how many redis paritions will be used.
// Setting more than one redis is useful in some cases that a single redis can't handle a the queue load either because of IO and memory restrictions or if possible CPU.
func (q *Queue) Urls(urls []string) {
	q.urls = q.urls[:0]
	q.pool = q.pool[:0]
	for _, v := range urls {
		c, e := redis.DialURL(v)
		checkErr(e)
		q.urls = append(q.urls, v)
		q.pool = append(q.pool, c)
	}
	q.paritions = len(q.urls)
}

// AddTask will add a task to the queue. It will accept an ID and a string.
// If more than one task are added with the same ID, queue will make sure they are send
// to the same analyser as long as analyers does not return before next ID is poped from the queue.
func (q *Queue) AddTask(id int, task string) {
	task = strconv.Itoa(id) + ";" + task
	queueName := "QUEUE"
	if q.QueueName != "" {
		queueName = q.QueueName
	}
	_, e := q.pool[id%q.paritions].Do("RPUSH", queueName, task)
	checkErr(e)
}

func (q *Queue) waitforSuccess(id int, success chan bool, pool map[int]chan string, queueName string) {
	redisdb, _ := redis.DialURL(q.urls[id%q.paritions])
	redisdb.Do("SET", queueName+"::PENDING::"+strconv.Itoa(id), 1)
	r := <-success
	if r {
		delete(pool, id)
		redisdb.Do("DEL", queueName+"::PENDING::"+strconv.Itoa(id))
	}
}

func removeTask(redisdb redis.Conn, queueName string) (int, string) {
	r, e := redisdb.Do("LPOP", queueName)
	checkErr(e)
	if r != nil {
		s, _ := redis.String(r, e)
		m := regexp.MustCompile(`(\d+);(.*)$`).FindStringSubmatch(s)
		id, _ := strconv.Atoi(m[1])
		redisdb.Do("SET", queueName+"::PENDING::"+strconv.Itoa(id), 1)
		return id, m[2]
	}
	return 0, ""
}

// AnalysePool will accept following paramters:
// n that is an int id which will specift which redis and which queue in that redis will be assign to this analyzer.
// poolSize which set max number of buffered Channels (concurrent workers) for analyzing the tasks.
// msgChannelBuff, an int that sets buffer size of msgChannel. Is Analyse operatino is slow setting msgChannelBuff to higher number will prevent execution bottle-neck in AnalysePool.
// exitOnEmpty is a closure function which will control if AnalysePool must exit when queue is empty. exitOnEmpty format is func() bool
// analyzer is a function with following parameters (id int, msg_channel chan string, success chan bool, next chan bool)
// analyzer will get ID of the task it is working on in id,
// msgChannel will be used to send related tasks to the analyzer function. There can be more than one task with the same ID if needed.
// success to indicate success or failure of analyzer.
// next to signal when it is OK for AnalysePool to start the next tasks. Normally this should be calls before ending analyzer.
// If a task does not return by sucess or crashes, all non-complete analyzes can be found by searching for PENDING::* keys in Redis. * will indicate the ID of failed tasks.
func (q *Queue) AnalysePool(analyzerID int, exitOnEmpty func() bool, analyzer func(int, chan string, chan bool, chan bool)) {
	redisdb, _ := redis.DialURL(q.urls[q.paritions%analyzerID])
	queueName := "QUEUE"
	if q.QueueName != "" {
		queueName = q.QueueName
	}
	buff := q.AnalyzerBuff
	if buff == 0 {
		buff = runtime.NumCPU()
	}
	next := make(chan bool, buff)
	pool := make(map[int]chan string)
	for {
		id, task := removeTask(redisdb, queueName)
		if task == "" {
			if exitOnEmpty() {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			if pool[id] == nil {
				pool[id] = make(chan string)
				success := make(chan bool)
				go analyzer(id, pool[id], success, next)
				go q.waitforSuccess(id, success, pool, queueName)
				pool[id] <- task
				next <- true
			} else {
				pool[id] <- task
			}
		}
	}

	for i := 0; i < q.AnalyzerBuff; i++ {
		next <- true
	}
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}
