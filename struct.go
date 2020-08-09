package go_delay_queue

import (
	"context"
	"sync"
	"time"
)

// 延迟等级定义
type DelayLevel struct {
	TopicName string //队列名
	Level int // 延迟等级
	RetryNums int // 重试次数
	NoAlive bool // 当为true 不获取延迟消息
	Ttl time.Duration // 延迟时间
	DealFn func(DelayTopicMsg) error // 处理方法
	TopicState topicState
}


type DelayServeConf struct {
	TopicDelayName string
	ClientCtx context.Context
	ClientWg *sync.WaitGroup
	DelayLevels []*DelayLevel
	Debug bool
}

// 延迟消息
type DelayTopicMsg struct {
	Id uint64 `json:"id" from:"id"`
	Level int `json:"level" from:"level"` // 延迟等级
	ExpiredAt int64 `json:"expired_at" from:"expired_at"`// 生效时间
	RetryNums int `json:"retry_nums" from:"retry_nums"` // 重试次数
	DelayMsg interface{} `json:"delay_msg" from:"delay_msg"` // 延迟消息
}
