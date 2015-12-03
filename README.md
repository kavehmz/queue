Queue
=========
[![Go Lang](http://kavehmz.github.io/static/gopher/gopher-front.svg)](https://golang.org/)
[![GoDoc](https://godoc.org/github.com/kavehmz/queue?status.svg)](https://godoc.org/github.com/kavehmz/queue)
[![Build Status](https://travis-ci.org/kavehmz/queue.svg?branch=master)](https://travis-ci.org/kavehmz/queue)
[![Coverage Status](https://coveralls.io/repos/kavehmz/queue/badge.svg?branch=master&service=github)](https://coveralls.io/github/kavehmz/queue?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/kavehmz/queue)

A [Go](http://golang.org) library for managing queues on top of Redis. 
It is based on a hiring exercise but later I found it useful for myself in a custom task processing project. 
I thought it might be useful in general.


## Installation

```bash
$ go get github.com/kavehmz/queue
```

# Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/kavehmz/queue"
)

func main() {
	var q queue.Queue
	q.Urls([]string{"redis://localhost:6379"})
	q.AddTask(1, "start")
	q.AddTask(2, "start")
	q.AddTask(1, "stop")
	q.AddTask(2, "stop")
	analyzer := func(id int, task chan string, success chan bool, next chan bool) {
		for {
			select {
			case msg := <-task:
				fmt.Println(id, msg)
				if msg == "stop" {
					<-next
					success <- true
					return
				}
			case <-time.After(2 * time.Second):
				fmt.Println("no new events for 2 seconds for ID", id)
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
}
```

## Approach

Focus of this design is mainly horizontal scalability via concurrency, partitioning and fault-detection.
