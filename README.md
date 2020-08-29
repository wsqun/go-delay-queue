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
	"errors"
	"fmt"
	dkafka "github.com/wsqun/go-delay-driver-kafka"
	dredis "github.com/wsqun/go-delay-driver-redis"
	go_delay_queue "github.com/wsqun/go-delay-queue"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main()  {
	var ctx,cancel = context.WithCancel(context.Background())
	var wg = &sync.WaitGroup{}
	// 实例化队列
	queuer,err := getQueuer("kafka", ctx, wg)
	if err != nil {
		panic(err)
	}

	// 创建延迟消息
	low := &go_delay_queue.DelayLevel{
		TopicName: "low",
		Level:     0,
		RetryNums: 1,
		Ttl:       5 * time.Second,
		DealFn: dealMsg,
	}
	medium := &go_delay_queue.DelayLevel{
		TopicName: "medium",
		Level:     1,
		RetryNums: 1,
		Ttl:       20 * time.Second,
		DealFn: dealMsg,
	}
	high := &go_delay_queue.DelayLevel{
		TopicName: "high",
		Level:     2,
		RetryNums: 1,
		Ttl:       40 * time.Second,
		DealFn: dealMsg,
	}
	delaySli := []*go_delay_queue.DelayLevel{
		low,medium,high,
	}
	// 调整最小延迟时间
	go_delay_queue.DurationMin = 1 * time.Second
	// 初始化配置
	conf := &go_delay_queue.DelayServeConf{
		ClientCtx:      context.Background(),
		ClientWg:       wg,
		DelayLevels: delaySli,
		Debug: true,
	}
	if serve,err := go_delay_queue.NewDelay(conf, queuer);err == nil {
		serve.Run()
		<-time.NewTimer(10 * time.Second).C
		err = serve.AddMsg(0, 1)
		fmt.Println(err)
	} else {
		panic(err)
	}
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("等待退出信号")
	<-sigterm
	cancel()
	fmt.Println("等待消费结束")
	wg.Wait()
	fmt.Println("消费结束")
}

func getQueuer(mq string, ctx context.Context, wg *sync.WaitGroup) (dr go_delay_queue.Iqueue, err error) {
	if mq == "redis" {
		dr,err = dredis.NewDredis("127.0.0.1:6379","")
		return
	}
	if mq == "kafka" {
		dr,err = dkafka.NewDKafka([]string{"192.168.1.106:9092"},"demo",ctx,wg)
		return
	}
	return
}

func dealMsg(dtm go_delay_queue.DelayTopicMsg) (err error) {
	fmt.Printf("开始处理消息：%#v\n", dtm)
	err = errors.New("retry ...")
	return
}
```