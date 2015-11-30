package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var testRedis = "redis://localhost:6379"

func TestPartitions(t *testing.T) {
	Partitions([]string{testRedis})
	_, err := redisPool[0].conn.Do("PING")
	if err != nil {
		t.Error("SetRedisPool items are not set correctly")
	}
}

func TestAddTask(t *testing.T) {
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	redisdb.Do("FLUSHALL")
	AddTask(4, "test")
	r, e := redisdb.Do("RPOP", "WAREHOUSE_0")
	s, e := redis.String(r, e)
	if s != "4;test" {
		t.Error("Task is stored incorrectly: ", s)
	}
}

func TestQueuesInPartision(t *testing.T) {
	QueuesInPartision(5)
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	redisdb.Do("FLUSHALL")
	AddTask(4, "test")
	r, e := redisdb.Do("RPOP", "WAREHOUSE_4")
	s, e := redis.String(r, e)
	if s != "4;test" {
		t.Error("Task is stored incorrectly: ", s)
	}

}

func TestAnalysePool(t *testing.T) {
	QueuesInPartision(1)
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	redisdb.Do("FLUSHALL")
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
	AddTask(2, "stop")
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
	AnalysePool(1, 2, true, analyzer)
	r, e := redisdb.Do("LLEN", "WAREHOUSE_0")
	s, e := redis.Int64(r, e)
	if s != 0 {
		t.Error("Queue is not empty after processing tasks: ", s)
	}

}

func TestAnalysePoolFailurePending(t *testing.T) {
	QueuesInPartision(1)
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	redisdb.Do("FLUSHALL")
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
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
	AnalysePool(1, 2, true, analyzer)
	r, e := redisdb.Do("GET", "PENDING::2")
	s, e := redis.Int(r, e)
	if s != 1 {
		t.Error("Task id 2 is not pending: ", s)
	}

}

func TestAnalysePoolCheckingWaiting(t *testing.T) {
	QueuesInPartision(1)
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	redisdb.Do("FLUSHALL")
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
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
	go AnalysePool(1, 2, false, analyzer)
	time.Sleep(100 * time.Millisecond)
	r, e := redisdb.Do("GET", "PENDING::2")
	s, e := redis.Int(r, e)
	if s != 1 {
		t.Error("Task id 2 is not pending after queue is empty: ", s)
	}
	AddTask(2, "stop")
	time.Sleep(200 * time.Millisecond)
	r, e = redisdb.Do("GET", "PENDING::2")
	s, e = redis.Int(r, e)
	if s != 0 {
		t.Error("Task 2 did not clear: ", s)
	}
}

func BenchmarkAddTask(b *testing.B) {
	QueuesInPartision(1)
	Partitions([]string{testRedis})
	for i := 0; i < b.N; i++ {
		AddTask(i, "stop")
	}
}

func BenchmarkRemoveTask(b *testing.B) {
	QueuesInPartision(1)
	Partitions([]string{testRedis})
	redisdb := redisPool[0].conn
	for i := 0; i < b.N; i++ {
		removeTask(redisdb, "WAREHOUSE_0")
	}
}

// This will act both as test and example in documentation
func ExampleAnalysePool() {
	QueuesInPartision(1)
	Partitions([]string{"redis://localhost:6379"})
	redisPool[0].conn.Do("FLUSHALL")
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
	AddTask(2, "stop")
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
	AnalysePool(1, 2, true, analyzer)
	// Output:
	// 1 start
	// 1 stop
	// 2 start
	// 2 stop

}
