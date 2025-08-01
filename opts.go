package batchx

import "time"

// Option 配置选项接口
type Option interface {
	apply(*options)
}

// options 配置选项结构体
type options struct {
	size          int           // 每个批次处理的消息数量
	buffer        int           // 每个worker的缓冲区大小
	worker        int           // worker数量
	interval      time.Duration // 批次处理的时间间隔
	deadLetterBuf int           // 死信队列缓冲区大小
}

// check 检查配置选项，为未设置的选项设置默认值
func (o *options) check() {
	if o.size <= 0 {
		o.size = 100 // 默认批次大小为100
	}
	if o.buffer <= 0 {
		o.buffer = 100 // 默认缓冲区大小为100
	}
	if o.worker <= 0 {
		o.worker = 5 // 默认worker数量为5
	}
	if o.interval <= 0 {
		o.interval = time.Second // 默认时间间隔为1秒
	}
	if o.deadLetterBuf <= 0 {
		o.deadLetterBuf = 100 // 默认死信队列缓冲区大小为100
	}
}

// funcOption 函数选项结构体，用于实现选项模式
type funcOption struct {
	f func(*options) // 应用配置的函数
}

// apply 应用配置到options结构体
func (fo *funcOption) apply(o *options) {
	fo.f(o)
}

// newOption 创建一个新的函数选项
func newOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// WithSize 设置每个批次处理的消息数量
func WithSize(s int) Option {
	return newOption(func(o *options) {
		o.size = s
	})
}

// WithBuffer 设置每个worker的缓冲区大小
func WithBuffer(b int) Option {
	return newOption(func(o *options) {
		o.buffer = b
	})
}

// WithWorker 设置worker数量
func WithWorker(w int) Option {
	return newOption(func(o *options) {
		o.worker = w
	})
}

// WithInterval 设置批次处理的时间间隔
func WithInterval(i time.Duration) Option {
	return newOption(func(o *options) {
		o.interval = i
	})
}

// WithDeadLetterBuf 设置死信队列缓冲区大小
func WithDeadLetterBuf(buf int) Option {
	return newOption(func(o *options) {
		o.deadLetterBuf = buf
	})
}
