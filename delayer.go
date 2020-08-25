package go_delay_queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sony/sonyflake"
	"log"
	"sync"
	"time"
)

type Delayer struct {
	levelTopicMap map[int]*DelayLevel
	levelMax      int
	clientCtx     context.Context
	clientWg      *sync.WaitGroup
	queuer        Iqueue
	now           int64 // 当前时间戳
	Debug         bool
}

var (
	DurationMin = 1 * time.Minute
	snow        *sonyflake.Sonyflake
)

func init() {
	t, _ := time.Parse("2006-01-02 15:04:05", "2018-01-01 00:00:00")
	snow = sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime: t,
	})
}

func NewDelay(conf *DelayServeConf, queuer Iqueue) (dr *Delayer, err error) {
	dr = &Delayer{
		clientCtx: conf.ClientCtx,
		clientWg:  conf.ClientWg,
		queuer:    queuer,
		levelMax:  0,
		Debug:     conf.Debug,
	}
	if err = dr.validateLevel(conf.DelayLevels); err != nil {
		return nil, err
	}
	go dr.realtime()
	return
}

// 校验等级
func (dr *Delayer) validateLevel(dl []*DelayLevel) (err error) {
	// 延迟消息等级需要从0开始 递增1
	levelNums := len(dl)
	dr.levelTopicMap = make(map[int]*DelayLevel, levelNums)
	if levelNums == 0 {
		err = errors.New("请先构造延迟等级")
		return err
	}
	for _, item := range dl {
		if item.Ttl < DurationMin {
			err = errors.New(fmt.Sprintf("延迟消息时长小于：%d", DurationMin))
			return err
		}

		if item.Level < 0 {
			err = errors.New(fmt.Sprintf("延迟消息等级小于0：%d", item.Level))
			return err
		}
		if _, exist := dr.levelTopicMap[item.Level]; exist {
			err = errors.New(fmt.Sprintf("延迟消息等级存在重复：%d", item.Level))
			return err
		}
		if item.Level > dr.levelMax {
			dr.levelMax = item.Level
		}
		dr.levelTopicMap[item.Level] = item
	}
	levelNums--
	if dr.levelMax != levelNums {
		err = errors.New("延迟消息等级需要从0开始 递增1")
		return err
	}
	return
}

func (dr *Delayer) Run() {
	// 开始订阅消息
	go dr.consumeDelayMsg()
}

// 消费队列延迟数据
func (dr *Delayer) consumeDelayMsg() {
	// 创建消费chan
	for _, item := range dr.levelTopicMap {
		if !item.NoAlive {
			go dr.queuer.SubscribeMsg(item.TopicName, dr.dealMsg)
			log.Println("开始订阅延迟Topic：", item.TopicName)
		}
	}
}

// 消息处理
func (dr *Delayer) dealMsg(data []byte) (err error) {
	var stru = &DelayTopicMsg{}
	if dr.Debug {
		log.Println("------- dealMsg -------- ")
		log.Printf("获得延迟数据：%s\n", data)
	}
	if err := json.Unmarshal(data, stru); err == nil {
		// 判断时间是否达到指定时间
		if stru.ExpiredAt > dr.now {
			var ttl = stru.ExpiredAt - dr.now
			if dr.Debug {
				log.Printf(fmt.Sprintf("消息未到达指定时间，等待 %ds\n", ttl))
			}
			select {
			case <-time.After(time.Duration(ttl) * time.Second):
			case <-dr.clientCtx.Done():
				// 消息重回
				err = errors.New("reload")
				return err
			}
		}
		// 可以开始消费
		go func(delayMsg *DelayTopicMsg) {
			err := dr.levelTopicMap[delayMsg.Level].DealFn(*delayMsg)
			if dr.Debug {
				log.Println("交付客户端处理:", err)
			}
			if err != nil {
				// 入下一等级消息
				dr.inQueue(true, delayMsg)
			}
		}(stru)
	} else {
		fmt.Println("解析异常:", err)
	}
	return
}

// 消息升级
func (dr *Delayer) inQueue(isUpgrade bool, dtm *DelayTopicMsg) (err error) {
	if isUpgrade {
		var overage = false
		// 判断重试次数
		if dtm.RetryNums >= dr.levelTopicMap[dtm.Level].RetryNums {
			overage = true
		}
		if overage {
			// 判断是否超过等级数量
			if (dtm.Level + 1) > dr.levelMax {
				if dr.Debug {
					log.Printf("无更高等级topic,丢弃: %#v\n", dtm)
				}
				return
			}
			dtm.Level++
			dtm.RetryNums = 0
		} else {
			dtm.RetryNums++
		}
	}
	dtm.ExpiredAt = time.Now().Add(dr.levelTopicMap[dtm.Level].Ttl).Unix()
	if msg, err := json.Marshal(dtm); err == nil {
		if dr.Debug {
			log.Println("------- inQueue -------- ")
			log.Printf("in queue:%s, %s\n", dr.levelTopicMap[dtm.Level].TopicName, msg)
		}
		err = dr.queuer.PublishMsg(dr.levelTopicMap[dtm.Level].TopicName, msg)
	} else {
		return err
	}
	return
}

// 添加延迟消息
func (dr *Delayer) AddMsg(level int, msg interface{}) (err error) {
	if level > dr.levelMax {
		err = errors.New(fmt.Sprintf("消息等级不能大于：%d", dr.levelMax))
		return
	}
	var dtm = &DelayTopicMsg{
		Id:        dr.getId(),
		Level:     level,
		RetryNums: 0,
		DelayMsg:  msg,
	}
	err = dr.inQueue(false, dtm)
	return
}

func (dr *Delayer) getId() uint64 {
	if id, err := snow.NextID(); err == nil {
		return id
	}
	return uint64(time.Now().UnixNano())
}
