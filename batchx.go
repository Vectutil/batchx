package batchx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// 定义错误类型
var (
	// ErrFull 表示通道已满的错误
	ErrFull          = errors.New("channel is full")
	// ErrBatcherClosed 表示批处理器已关闭的错误
	ErrBatcherClosed = errors.New("batcher is closed")
)

// msg 消息结构体，包含键和值
type msg struct {
	key string      // 消息的键
	val interface{} // 消息的值
}

// channelWithLen 带长度计数的通道结构体
type channelWithLen struct {
	ch  chan *msg // 消息通道
	len int32     // 通道中消息的数量（原子计数）
}

// Batcher 批处理器结构体
type Batcher struct {
	opts          options                                    // 配置选项
	Do            func(ctx context.Context, val map[string][]interface{}) error // 批处理函数
	ErrCallbackDo func(ctx context.Context, val map[string][]interface{}, err error) // 错误回调函数
	chans         []*channelWithLen                          // worker通道数组
	deadLetCh     chan map[string][]interface{}              // 死信队列通道
	msgPool       sync.Pool                                  // 消息对象池
	wait          sync.WaitGroup                             // 等待组，用于等待所有goroutine完成
	closed        int32                                      // 是否已关闭的标志（原子操作）
}

// New 创建一个新的批处理器
// opts: 配置选项，可以设置批次大小、缓冲区大小、worker数量等
func New(opts ...Option) *Batcher {
	b := &Batcher{}
	// 应用配置选项
	for _, opt := range opts {
		opt.apply(&b.opts)
	}
	// 检查并设置默认配置
	b.opts.check()

	// 初始化worker通道
	b.chans = make([]*channelWithLen, b.opts.worker)
	for i := 0; i < b.opts.worker; i++ {
		b.chans[i] = &channelWithLen{
			ch:  make(chan *msg, b.opts.buffer), // 创建带缓冲的通道
			len: 0,                              // 初始化长度为0
		}
	}
	
	// 创建死信队列通道
	b.deadLetCh = make(chan map[string][]interface{}, b.opts.deadLetterBuf)
	
	// 初始化消息对象池，用于复用msg对象，减少GC压力
	b.msgPool = sync.Pool{
		New: func() interface{} {
			return &msg{}
		},
	}
	
	return b
}

// Start 启动批处理器
// 会启动多个goroutine来处理消息，包括worker goroutine和死信队列处理goroutine
func (b *Batcher) Start() {
	// 检查处理函数是否已设置
	if b.Do == nil {
		panic("batcher: Do func is nil")
	}
	
	// 添加等待的goroutine数量（worker数量+死信队列处理goroutine）
	b.wait.Add(len(b.chans) + 1)
	
	// 为每个worker启动一个merge goroutine处理消息
	for i, ch := range b.chans {
		go b.merge(i, ch.ch)
	}
	
	// 启动死信队列处理goroutine
	go b.processDeadLetters()
}

// processDeadLetters 处理死信队列中的消息批次
// 当批处理失败时，消息会被发送到死信队列，该函数负责处理这些消息
func (b *Batcher) processDeadLetters() {
	// 函数结束时标记等待组完成
	defer b.wait.Done()

	// 持续从死信队列通道中读取批次消息并处理
	for batch := range b.deadLetCh {
		// 如果设置了错误回调函数，则调用它处理死信
		if b.ErrCallbackDo != nil {
			b.ErrCallbackDo(context.Background(), batch, errors.New("processing dead letter"))
		}
	}
}

// findLightestWorker 查找负载最轻的worker
// 通过比较各个worker通道中的消息数量，返回消息数量最少的worker索引和通道
func (b *Batcher) findLightestWorker() (int, *channelWithLen) {
	minIdx := 0
	// 使用原子操作加载第一个通道的消息数量
	minLen := atomic.LoadInt32(&b.chans[0].len)

	// 遍历其余通道，找到消息数量最少的通道
	for i := 1; i < len(b.chans); i++ {
		// 使用原子操作加载当前通道的消息数量
		currLen := atomic.LoadInt32(&b.chans[i].len)
		if currLen < minLen {
			minIdx = i
			minLen = currLen
		}
	}

	// 返回负载最轻的worker索引和通道
	return minIdx, b.chans[minIdx]
}

// getMsg 从对象池中获取一个msg对象
// 使用对象池可以减少GC压力，提高性能
func (b *Batcher) getMsg() *msg {
	return b.msgPool.Get().(*msg)
}

// putMsg 将msg对象重置并放回对象池
// 重置对象状态以避免内存泄漏和数据污染
func (b *Batcher) putMsg(m *msg) {
	m.key = ""      // 重置key
	m.val = nil     // 重置val
	b.msgPool.Put(m) // 放回对象池
}

// Add 向批处理器添加一个消息
// key: 消息的键，用于分组
// val: 消息的值
// 返回错误信息，如果批处理器已关闭或通道已满则返回相应错误
func (b *Batcher) Add(key string, val interface{}) error {
	// 检查批处理器是否已关闭
	if atomic.LoadInt32(&b.closed) == 1 {
		return ErrBatcherClosed
	}

	// 查找负载最轻的worker
	_, lightest := b.findLightestWorker()

	// 检查通道是否已满
	if atomic.LoadInt32(&lightest.len) >= int32(b.opts.buffer) {
		return ErrFull
	}

	// 从对象池获取消息对象并设置值
	m := b.getMsg()
	m.key = key
	m.val = val

	// 增加通道长度计数
	atomic.AddInt32(&lightest.len, 1)
	
	// 尝试将消息发送到通道
	select {
	case lightest.ch <- m:
		// 发送成功，返回nil
		return nil
	default:
		// 发送失败（通道已满），回退长度计数并放回对象池
		atomic.AddInt32(&lightest.len, -1)
		b.putMsg(m)
		return ErrFull
	}
}

// merge 合并并处理消息的主函数
// idx: worker索引
// ch: 要处理的消息通道
func (b *Batcher) merge(idx int, ch <-chan *msg) {
	// 函数结束时标记等待组完成
	defer b.wait.Done()

	var (
		count  int                                // 当前批次中的消息数量
		vals   = make(map[string][]interface{}, b.opts.size) // 存储消息的map，按键分组
		ticker = time.NewTicker(b.opts.interval) // 定时器，用于按时间间隔处理批次
	)

	// 函数结束时停止定时器
	defer ticker.Stop()

	// 持续循环处理消息
	for {
		select {
		// 从通道接收消息
		case m, ok := <-ch:
			// 通道已关闭
			if !ok {
				// 如果还有未处理的消息，执行处理
				if len(vals) > 0 {
					if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
						b.ErrCallbackDo(context.Background(), vals, err)
					}
				}
				return
			}
			
			// 减少通道长度计数
			atomic.AddInt32(&b.chans[idx].len, -1)
			
			// 增加当前批次消息计数
			count++
			
			// 将消息按key分组存储
			vals[m.key] = append(vals[m.key], m.val)
			
			// 将消息对象放回对象池
			b.putMsg(m)
			
			// 如果当前批次消息数量达到设定的批次大小，则执行处理
			if count >= b.opts.size {
				if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
					b.ErrCallbackDo(context.Background(), vals, err)
				}
				// 重置批次数据
				vals = make(map[string][]interface{}, b.opts.size)
				count = 0
			}
			
		// 定时器触发，按时间间隔处理批次
		case <-ticker.C:
			// 如果当前批次有消息，则执行处理
			if len(vals) > 0 {
				if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
					b.ErrCallbackDo(context.Background(), vals, err)
				}
				// 重置批次数据
				vals = make(map[string][]interface{}, b.opts.size)
				count = 0
			}
		}
	}
}

// Close 关闭批处理器
// 会关闭所有worker通道和死信队列通道，并等待所有goroutine完成
func (b *Batcher) Close() {
	// 使用原子操作确保只关闭一次
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return
	}
	
	// 关闭所有worker通道
	for _, ch := range b.chans {
		close(ch.ch)
	}
	
	// 关闭死信队列通道
	close(b.deadLetCh)
	
	// 等待所有goroutine完成
	b.wait.Wait()
}
