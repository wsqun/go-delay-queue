package go_delay_queue

type Iqueue interface {
	// 订阅消息
	SubscribeMsg(topic string, dealFn func([]byte) (err error)) (err error)
	// 生产消息
	PublishMsg(topic string, msg []byte) (err error)
}