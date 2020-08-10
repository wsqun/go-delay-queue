package go_delay_queue

type Iqueue interface {
	// 订阅消息
	// topic 为队列名，dealFn为消息处理逻辑方法，[]byte为队列消息，当err发生则放回消息，默认为程序退出
	SubscribeMsg(topic string, dealFn func([]byte) (err error)) (err error)
	// 生产消息
	PublishMsg(topic string, msg []byte) (err error)
}