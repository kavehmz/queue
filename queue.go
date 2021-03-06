/*
Package queue is a simple Queue system written in Go that will uses Redis.
Focus of this design is mainly horisontal scalability via concurrency, partitioning and fault-detection
Queues can be partitions in to more than one Redis if necessary.

Number of redis partitions is set by using Urls function and setting slice of Redis URL connections.
Redis partitioning is required in cases that one redis cannot handle the load because of IO, moemory or in rare situations CPU limitations.

In case of crash record of all incomplete tasks will be kepts in redis as keys with this format
	QUEUE::0::PENDING::ID
ID will indicate the ID of failed tasks.

To use this library you need to use queue struct.

	var q Queue
	q.Urls([]{redis://localhost:6379})

Adding tasks is done by calling AddTask. This function will accept an ID and the task itself that will be as a string.

	q.AddTask(1, "task1")
	q.AddTask(2, "task2")

ID can be used in a special way. If ID of two tasks are the same while processing AnalysePool will send them to the same analyser goroutine if analyzer waits enough.

	q.AddTask(2, "start")
	q.AddTask(1, "load")
	q.AddTask(2, "load")
	q.AddTask(1, "stop")
	q.AddTask(2, "stop")

This feature can be used in analyser to process a set of related tasks one after another.
If you are adding ID related tasks and you need to spinup more than one AnalysePool to fetch and distribute tasks you need to insert the tasks into separate queue or separate redis servers.
To have separate queues you can set Queues number in the queue strcuture.

	whichQueue=id % q.Queues

AnalysePool accepts 3 parameters. One analyzerID that will identify which redis pool this AnalysePool will connect to.

	whichRedis=(analyzerID/q.Queues) % len(q.urls)

AnalysePool need two closures, analyzer and exitOnEmpty. Format of those closure are as follows.

	analyzer := func(id int, task chan string, success chan bool) {
		for {
			select {
			case msg := <-task:
				//process the task
				if msg == "stop_indicator" {
					success <- true
					return
				}
			}
		}
	}
	exitOnEmpty := func() bool {
		return true
	}
	q.AnalysePool(1, exitOnEmpty, analyzer)
*/
package queue

import (
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

//Queue the strcuture that will ecompass the queue settings and methods.
type Queue struct {
	// AnalyzeBuff will set number of concurrent running anlyzers. It will default to number of cpu if not set.
	AnalyzerBuff int
	// QueueName this will set the name used in udnerlying system for the queue. Default is "QUEUE"
	QueueName string
	// Number of queues in each redis server. This is useful if you have ID related tasks and you need more than one AnalysePool. Default is 1
	Queues int
	urls   []string
	pool   []redis.Conn
}

func (q *Queue) queues() int {
	if q.Queues != 0 {
		return q.Queues
	}
	return 1
}

func (q *Queue) pendingKeyName(id int) string {
	return q.queueName(id) + "::PENDING::" + strconv.Itoa(id)
}

func (q *Queue) redisID(id int) int {
	return (id / q.queues()) % len(q.urls)
}

func (q *Queue) queueName(id int) string {
	if q.QueueName != "" {
		return q.QueueName + "::" + strconv.Itoa(id%q.queues())
	}
	return "QUEUE" + "::" + strconv.Itoa(id%q.queues())
}

func (q *Queue) analyzerBuff() int {
	if q.AnalyzerBuff != 0 {
		return q.AnalyzerBuff
	}
	return runtime.NumCPU()
}

// Urls will accept a slice of redis connection URLS. This slice will setup the connections and also set how many redis partitions will be used.
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
}

// AddTask will add a task to the queue. It will accept an ID and a string.
// If more than one task are added with the same ID, queue will make sure they are send
// to the same analyser as long as analyers does not return before next ID is poped from the queue.
func (q *Queue) AddTask(id int, task string) {
	task = strconv.Itoa(id) + ";" + task
	_, e := q.pool[q.redisID(id)].Do("RPUSH", q.queueName(id), task)
	checkErr(e)
}

func (q *Queue) waitforSuccess(id int, success chan bool, pool map[int]chan string, next chan bool) {
	redisdb, _ := redis.DialURL(q.urls[q.redisID(id)])
	redisdb.Do("SET", q.pendingKeyName(id), 1)
	r := <-success
	if r {
		delete(pool, id)
		redisdb.Do("DEL", q.pendingKeyName(id))
	}
	<-next
}

func (q *Queue) removeTask(redisdb redis.Conn, queueName string) (int, string) {
	r, e := redisdb.Do("LPOP", queueName)
	checkErr(e)
	if r != nil {
		s, _ := redis.String(r, e)
		m := regexp.MustCompile(`(\d+);(.*)$`).FindStringSubmatch(s)
		id, _ := strconv.Atoi(m[1])
		redisdb.Do("SET", q.pendingKeyName(id), 1)
		return id, m[2]
	}
	return 0, ""
}

/*
AnalysePool can be calls to process redis queue(s).
analyzerID will set which redis AnalysePool will connect to (redis:=pool[len(urls)%AnalysePool])

exitOnEmpty is a closure function which will control inner loop of AnalysePool when queue is empty.
	exitOnEmpty := func() bool {
		return true
	}
analyzer is a closure function which will be called for processing the tasks popped from queue.
	analyzer := func(id int, task chan string, success chan bool) {
		for {
			select {
			case msg := <-task:
				if id == 2 {
					time.Sleep(20 * time.Millisecond)
				}
				fmt.Println(id, msg)
				if msg == "stop" {
					success <- true
					return
				}
			case <-time.After(2 * time.Second):
				fmt.Println("no new event for 2 seconds for ID", id)
				success <- false
				return
			}
		}
	}
Analyser clousre must be able to accept the new Tasks without delay and if needed process them concurrently. Delay in accepting new Task will block AnalysePool.
*/
func (q *Queue) AnalysePool(analyzerID int, exitOnEmpty func() bool, analyzer func(int, chan string, chan bool)) {
	redisdb, _ := redis.DialURL(q.urls[q.redisID(analyzerID)])

	next := make(chan bool, q.analyzerBuff())
	pool := make(map[int]chan string)
	for {
		id, task := q.removeTask(redisdb, q.queueName(analyzerID))
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
				go analyzer(id, pool[id], success)
				go q.waitforSuccess(id, success, pool, next)
				pool[id] <- task
				next <- true
			} else {
				pool[id] <- task
			}
		}
	}

	for i := 0; i < q.analyzerBuff(); i++ {
		next <- true
	}
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}
