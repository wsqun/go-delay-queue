package go_delay_queue

import (
	"sync"
	"time"
)

var once = &sync.Once{}
// 实时更新当前时间戳
func (dr *Delayer) realtime()  {
	once.Do(func() {
		dr.now= time.Now().Unix()
		timer := time.NewTicker(1 * time.Second)
		for ;; {
			<-timer.C
			dr.now++
		}
	})
}