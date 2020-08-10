# go-delay-queue

## WIKI
https://github.com/wsqun/go-delay-queue/wiki/Introduction

## Installation
make sure to initialize a Go module before installing go-delay-queue:
```
go get https://github.com/wsqun/go-delay-queue
```
Import
```
import "github.com/wsqun/go-delay-queue"
```

## QuickStart
``` go
package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	dredis "github.com/wsqun/go-delay-driver-redis"
	go_delay_queue "github.com/wsqun/go-delay-queue"
	"sync"
	"time"
)

func main()  {
	// 实例化队列
	queuer,err := dredis.NewDredis(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if err != nil {
		panic(err)
	}

	// 创建延迟消息
	low := &go_delay_queue.DelayLevel{
		TopicName: "log",
		Level:     0,
		RetryNums: 1,
		Ttl:       1 * time.Minute,
		DealFn: dealMsg,
	}
	medium := &go_delay_queue.DelayLevel{
		TopicName: "medium",
		Level:     1,
		RetryNums: 1,
		Ttl:       5 * time.Minute,
		DealFn: dealMsg,
	}
	high := &go_delay_queue.DelayLevel{
		TopicName: "high",
		Level:     2,
		RetryNums: 1,
		Ttl:       1 * time.Hour,
		DealFn: dealMsg,
	}
	delaySli := []*go_delay_queue.DelayLevel{
		low,medium,high,
	}
	// 初始化配置
	var wg = &sync.WaitGroup{}
	conf := &go_delay_queue.DelayServeConf{
		ClientCtx:      context.Background(),
		ClientWg:       wg,
		DelayLevels: delaySli,
		Debug: true,
	}
	if serve,err := go_delay_queue.NewDelay(conf, queuer);err == nil {
		serve.Run()
		serve.AddMsg(0, 1)
	} else {
		panic(err)
	}
	wg.Add(1)
	wg.Wait()
}

func dealMsg(dtm go_delay_queue.DelayTopicMsg) (err error) {
	fmt.Printf("开始处理消息：%#v\n", dtm)
	return
}
```