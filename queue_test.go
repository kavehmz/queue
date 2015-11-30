package queue

import (
	"fmt"
	"testing"

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
	redisdb.Do("DEL", "WAREHOUSE_0")
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
	redisdb.Do("DEL", "WAREHOUSE_4")
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
	redisdb.Do("DEL", "WAREHOUSE_0")
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
	AddTask(2, "stop")
	a := analyse
	AnalysePool(1, 2, true, a)
	r, e := redisdb.Do("LLEN", "WAREHOUSE_0")
	s, e := redis.Int64(r, e)
	if s != 0 {
		t.Error("Queue is not empty after processing tasks: ", s)
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
	Partitions([]string{testRedis})
	AddTask(1, "start")
	AddTask(2, "start")
	AddTask(1, "stop")
	AddTask(2, "stop")
	a := analyse
	AnalysePool(1, 2, true, a)
	// Output:
	// 1 start
	// 2 start
	// 1 stop
	// 2 stop

}

func analyse(id int, msg_channel chan string, success chan bool, next chan bool) {
	for {
		select {
		case msg := <-msg_channel:
			fmt.Println(id, msg)
			if msg == "stop" {
				<-next
				success <- true
				return
			}
		}
	}
}
