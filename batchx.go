package batchx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrFull          = errors.New("channel is full")
	ErrBatcherClosed = errors.New("batcher is closed")
)

type msg struct {
	key string
	val interface{}
}

type channelWithLen struct {
	ch  chan *msg
	len int32
}

type Batcher struct {
	opts          options
	Do            func(ctx context.Context, val map[string][]interface{}) error
	ErrCallbackDo func(ctx context.Context, val map[string][]interface{}, err error)
	chans         []*channelWithLen
	deadLetCh     chan map[string][]interface{}
	msgPool       sync.Pool
	wait          sync.WaitGroup
	closed        int32
}

func New(opts ...Option) *Batcher {
	b := &Batcher{}
	for _, opt := range opts {
		opt.apply(&b.opts)
	}
	b.opts.check()

	b.chans = make([]*channelWithLen, b.opts.worker)
	for i := 0; i < b.opts.worker; i++ {
		b.chans[i] = &channelWithLen{
			ch:  make(chan *msg, b.opts.buffer),
			len: 0,
		}
	}
	b.deadLetCh = make(chan map[string][]interface{}, b.opts.deadLetterBuf)
	b.msgPool = sync.Pool{
		New: func() interface{} {
			return &msg{}
		},
	}
	return b
}

func (b *Batcher) Start() {
	if b.Do == nil {
		panic("batcher: Do func is nil")
	}
	b.wait.Add(len(b.chans) + 1)
	for i, ch := range b.chans {
		go b.merge(i, ch.ch)
	}
	go b.processDeadLetters()
}

func (b *Batcher) processDeadLetters() {
	defer b.wait.Done()

	for batch := range b.deadLetCh {
		if b.ErrCallbackDo != nil {
			b.ErrCallbackDo(context.Background(), batch, errors.New("processing dead letter"))
		}
	}
}

func (b *Batcher) findLightestWorker() (int, *channelWithLen) {
	minIdx := 0
	minLen := atomic.LoadInt32(&b.chans[0].len)

	for i := 1; i < len(b.chans); i++ {
		currLen := atomic.LoadInt32(&b.chans[i].len)
		if currLen < minLen {
			minIdx = i
			minLen = currLen
		}
	}

	return minIdx, b.chans[minIdx]
}

func (b *Batcher) getMsg() *msg {
	return b.msgPool.Get().(*msg)
}

func (b *Batcher) putMsg(m *msg) {
	m.key = ""
	m.val = nil
	b.msgPool.Put(m)
}

func (b *Batcher) Add(key string, val interface{}) error {
	if atomic.LoadInt32(&b.closed) == 1 {
		return ErrBatcherClosed
	}

	_, lightest := b.findLightestWorker()

	if atomic.LoadInt32(&lightest.len) >= int32(b.opts.buffer) {
		return ErrFull
	}

	m := b.getMsg()
	m.key = key
	m.val = val

	atomic.AddInt32(&lightest.len, 1)
	select {
	case lightest.ch <- m:
		return nil
	default:
		atomic.AddInt32(&lightest.len, -1)
		b.putMsg(m)
		return ErrFull
	}
}

func (b *Batcher) merge(idx int, ch <-chan *msg) {
	defer b.wait.Done()

	var (
		count  int
		vals   = make(map[string][]interface{}, b.opts.size)
		ticker = time.NewTicker(b.opts.interval)
	)

	defer ticker.Stop()

	for {
		select {
		case m, ok := <-ch:
			if !ok {
				if len(vals) > 0 {
					if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
						b.ErrCallbackDo(context.Background(), vals, err)
					}
				}
				return
			}
			atomic.AddInt32(&b.chans[idx].len, -1)
			count++
			vals[m.key] = append(vals[m.key], m.val)
			b.putMsg(m)
			if count >= b.opts.size {
				if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
					b.ErrCallbackDo(context.Background(), vals, err)
				}
				vals = make(map[string][]interface{}, b.opts.size)
				count = 0
			}
		case <-ticker.C:
			if len(vals) > 0 {
				if err := b.Do(context.Background(), vals); err != nil && b.ErrCallbackDo != nil {
					b.ErrCallbackDo(context.Background(), vals, err)
				}
				vals = make(map[string][]interface{}, b.opts.size)
				count = 0
			}
		}
	}
}

func (b *Batcher) Close() {
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return
	}
	for _, ch := range b.chans {
		close(ch.ch)
	}
	close(b.deadLetCh)
	b.wait.Wait()
}
