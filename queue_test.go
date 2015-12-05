package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var testRedis = "redis://localhost:6379"

func TestQueue_Urls(t *testing.T) {
	var q Queue
	q.Urls([]string{testRedis})
	_, err := q.pool[0].Do("PING")
	if err != nil {
		t.Error("SetRedisPool items are not set correctly")
	}
}

func TestBadURL(t *testing.T) {
	paniced := false
	defer func() {
		e := recover()
		if e != nil {
			paniced = true
		}
	}()
	var q Queue
	q.Urls([]string{"redis://127.0.0.1:0"})
	if !paniced {
		t.Error("Accepted a bad URL without panic")
	}
}

func TestQueue_AddTask(t *testing.T) {
	var q Queue
	q.Urls([]string{testRedis})
	redisdb := q.pool[0]
	redisdb.Do("FLUSHALL")
	q.AddTask(4, "test")
	r, e := redisdb.Do("RPOP", "QUEUE::0")
	s, e := redis.String(r, e)
	if s != "4;test" {
		t.Error("Task is stored incorrectly: ", s)
	}
}

func TestQueue_QueueName(t *testing.T) {
	var q Queue
	if q.queueName(5) != "QUEUE::0" {
		t.Error("Queue name is wrong for id 5 and queues default: ", q.queueName(5))
	}

	q.Queues = 1
	if q.queueName(5) != "QUEUE::0" {
		t.Error("Queue name is wrong for id 5 and queues 1: ", q.queueName(5))
	}

	q.Queues = 2
	if q.queueName(5) != "QUEUE::1" {
		t.Error("Queue name is wrong for id 5 and queues 1: ", q.queueName(5))
	}
}

func TestQueue_AnalysePool(t *testing.T) {
	var q Queue
	q.Urls([]string{testRedis})
	redisdb := q.pool[0]
	redisdb.Do("FLUSHALL")
	q.QueueName = "CUSTOM"
	q.AddTask(1, "start")
	q.AddTask(2, "start")
	q.AddTask(1, "stop")
	q.AddTask(2, "stop")
	analyzer := func(id int, msg_channel chan string, success chan bool, next chan bool) {
		for {
			select {
			case msg := <-msg_channel:
				if msg == "stop" {
					<-next
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
	r, e := redisdb.Do("LLEN", "QUEUE::0")
	s, e := redis.Int64(r, e)
	if s != 0 {
		t.Error("Queue is not empty after processing tasks: ", s)
	}

}

func TestAnalysePoolFailurePending(t *testing.T) {
	var q Queue
	q.Urls([]string{testRedis})
	redisdb := q.pool[0]
	redisdb.Do("FLUSHALL")
	q.AddTask(1, "start")
	q.AddTask(2, "start")
	q.AddTask(1, "stop")
	analyzer := func(id int, msg_channel chan string, success chan bool, next chan bool) {
		for {
			select {
			case msg := <-msg_channel:
				if msg == "stop" {
					<-next
					success <- true
					return
				}
			case <-time.After(1 * time.Second):
				fmt.Println("no new event for 2 seconds for ID", id)
				<-next
				success <- false
				return
			}
		}
	}
	exitOnEmpty := func() bool {
		return true
	}
	q.AnalysePool(1, exitOnEmpty, analyzer)
	r, e := redisdb.Do("GET", "QUEUE::0::PENDING::2")
	s, e := redis.Int(r, e)
	if s != 1 {
		t.Error("Task id 2 is not pending: ", s)
	}

}

func TestAnalysePoolCheckingWaiting(t *testing.T) {
	var q Queue
	q.AnalyzerBuff = 2
	q.Urls([]string{testRedis})
	redisdb := q.pool[0]
	redisdb.Do("FLUSHALL")
	q.AddTask(1, "start")
	q.AddTask(2, "start")
	q.AddTask(1, "stop")
	analyzer := func(id int, msg_channel chan string, success chan bool, next chan bool) {
		for {
			select {
			case msg := <-msg_channel:
				if msg == "stop" {
					<-next
					success <- true
					return
				}
			}
		}
	}
	exit := false
	exitOnEmpty := func() bool {
		return exit
	}
	go q.AnalysePool(1, exitOnEmpty, analyzer)
	time.Sleep(100 * time.Millisecond)
	r, e := redisdb.Do("GET", "QUEUE::0::PENDING::2")
	s, e := redis.Int(r, e)
	if s != 1 {
		t.Error("Task id 2 is not pending after queue is empty: ", s)
	}
	q.AddTask(2, "stop")
	time.Sleep(200 * time.Millisecond)
	r, e = redisdb.Do("GET", "QUEUE::PENDING::2")
	s, e = redis.Int(r, e)
	if s != 0 {
		t.Error("Task 2 did not clear: ", s)
	}
	exit = true
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkQueue_AddTask(b *testing.B) {
	var q Queue
	q.Urls([]string{testRedis})
	q.pool[0].Do("FLUSHALL")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.AddTask(i, "stop")
	}
}

func BenchmarkRemoveTask(b *testing.B) {
	var q Queue
	q.Urls([]string{testRedis})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, s := q.removeTask(q.pool[0], q.queueName(1))
		if s == "" {
			panic("Reached an empty queue. Benchmark is not valid:")
		}
	}
}

// This will act both as test and example in documentation
func ExampleQueue_AnalysePool() {
	var q Queue
	q.Urls([]string{testRedis})
	q.pool[0].Do("FLUSHALL")
	q.AddTask(1, "start")
	q.AddTask(2, "start")
	q.AddTask(1, "stop")
	q.AddTask(2, "stop")
	analyzer := func(id int, msg_channel chan string, success chan bool, next chan bool) {
		for {
			select {
			case msg := <-msg_channel:
				if id == 2 {
					time.Sleep(20 * time.Millisecond)
				}
				fmt.Println(id, msg)
				if msg == "stop" {
					<-next
					success <- true
					return
				}
			case <-time.After(2 * time.Second):
				fmt.Println("no new event for 2 seconds for ID", id)
				<-next
				success <- false
				return
			}
		}
	}
	exitOnEmpty := func() bool {
		return true
	}
	q.AnalysePool(1, exitOnEmpty, analyzer)
	// Output:
	// 1 start
	// 1 stop
	// 2 start
	// 2 stop

}
