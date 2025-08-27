# batchx

实现基于内存的批量处理队列

## 简介

batchx 是一个基于 Go 语言开发的高性能批量处理库。它能够将大量独立的消息或任务合并成批次进行处理，从而显著提高处理效率，特别是在高并发场景下。该库具有以下特点：

- 高性能：使用 sync.Pool 复用对象，降低 GC 压力
- 可配置：支持自定义批次大小、缓冲区大小、worker 数量等参数
- 负载均衡：自动将消息分发到负载最轻的 worker
- 错误处理：支持死信队列处理失败的批次
- 超时触发：支持按时间间隔自动触发批次处理

## 安装

```bash
go get github.com/Vectutil/batchx
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/Vectutil/batchx"
)

func main() {
    // 创建一个批处理器
    batcher := batchx.New(
        batchx.WithSize(10),        // 每批次处理10个消息
        batchx.WithBuffer(100),     // 每个worker的缓冲区大小为100
        batchx.WithWorker(5),       // 使用5个worker并发处理
        batchx.WithInterval(1*time.Second), // 或者每秒处理一次
    )
    
    // 设置处理函数
    batcher.Do = func(ctx context.Context, vals map[string][]interface{}) error {
        fmt.Printf("处理批次，共 %d 个键\n", len(vals))
        for key, items := range vals {
            fmt.Printf("键: %s, 数量: %d\n", key, len(items))
        }
        return nil
    }
    
    // 启动批处理器
    batcher.Start()
    
    // 添加消息
    for i := 0; i < 100; i++ {
        batcher.Add("key1", fmt.Sprintf("value-%d", i))
        if i%3 == 0 {
            batcher.Add("key2", fmt.Sprintf("value-%d", i))
        }
    }
    
    // 等待处理完成
    time.Sleep(2 * time.Second)
    
    // 关闭批处理器
    batcher.Close()
}
```

## 业务场景示例

### 1. 日志批量写入数据库

在高并发系统中，频繁地写入日志到数据库会影响性能。使用 batchx 可以将日志批量写入数据库：

```go
// 创建批处理器
logBatcher := batchx.New(
    batchx.WithSize(50),        // 每批次处理50条日志
    batchx.WithWorker(3),       // 3个worker并发处理
    batchx.WithInterval(5*time.Second), // 或者每5秒处理一次
)

// 设置日志处理函数
logBatcher.Do = func(ctx context.Context, logs map[string][]interface{}) error {
    // 将日志批量写入数据库
    var batchLogs []LogEntry
    for _, items := range logs {
        for _, item := range items {
            if log, ok := item.(LogEntry); ok {
                batchLogs = append(batchLogs, log)
            }
        }
    }
    
    return db.BatchInsertLogs(batchLogs)
}

// 启动批处理器
logBatcher.Start()

// 在业务代码中添加日志
logBatcher.Add("user_action", LogEntry{
    UserID:    userID,
    Action:    "click",
    Timestamp: time.Now(),
})
```

### 2. 消息通知批量发送

当需要发送大量通知消息时，可以使用 batchx 批量发送以提高效率：

```go
// 创建消息批处理器
notificationBatcher := batchx.New(
    batchx.WithSize(100),       // 每批次处理100条消息
    batchx.WithWorker(2),       // 2个worker并发处理
    batchx.WithInterval(30*time.Second), // 或者每30秒处理一次
)

// 设置消息处理函数
notificationBatcher.Do = func(ctx context.Context, notifications map[string][]interface{}) error {
    // 按类型批量发送通知
    for notifyType, items := range notifications {
        var messages []Message
        for _, item := range items {
            if msg, ok := item.(Message); ok {
                messages = append(messages, msg)
            }
        }
        
        switch notifyType {
        case "email":
            emailService.BatchSend(messages)
        case "sms":
            smsService.BatchSend(messages)
        }
    }
    return nil
}

// 启动批处理器
notificationBatcher.Start()

// 添加通知消息
notificationBatcher.Add("email", Message{
    To:      "user@example.com",
    Subject: "系统通知",
    Content: "您有一条新消息",
})
```

### 3. 数据统计聚合

在需要定期统计某些数据指标时，可以使用 batchx 聚合数据：

```go
// 创建统计数据批处理器
statsBatcher := batchx.New(
    batchx.WithSize(200),       // 每批次处理200条统计数据
    batchx.WithWorker(1),       // 1个worker处理
    batchx.WithInterval(1*time.Minute), // 每分钟处理一次
)

// 设置统计数据处理函数
statsBatcher.Do = func(ctx context.Context, stats map[string][]interface{}) error {
    // 聚合统计数据
    aggregated := make(map[string]int)
    for statType, items := range stats {
        for _, item := range items {
            if count, ok := item.(int); ok {
                aggregated[statType] += count
            }
        }
    }
    
    // 将聚合后的数据存储到数据库
    return statsService.SaveAggregatedData(aggregated)
}

// 启动批处理器
statsBatcher.Start()

// 添加统计数据
statsBatcher.Add("page_views", 1)
statsBatcher.Add("user_registrations", 1)
```

## 配置选项

- `WithSize(size int)`: 设置每个批次处理的消息数量，默认为100
- `WithBuffer(buffer int)`: 设置每个worker的缓冲区大小，默认为100
- `WithWorker(worker int)`: 设置worker数量，默认为5
- `WithInterval(interval time.Duration)`: 设置批次处理的时间间隔，默认为1秒
- `WithDeadLetterBuf(buf int)`: 设置死信队列缓冲区大小，默认为100

## 错误处理

batchx 支持错误处理机制，可以通过设置 `ErrCallbackDo` 函数来处理处理失败的批次：

```go
batcher.ErrCallbackDo = func(ctx context.Context, vals map[string][]interface{}, err error) {
    // 将处理失败的批次发送到死信队列或其他处理机制
    fmt.Printf("处理批次失败: %v\n", err)
    // 可以将失败的批次重新入队或者记录到日志中
}
```

## 性能优化建议

1. 根据业务场景合理设置批次大小，过小的批次无法发挥批量处理的优势，过大的批次可能导致内存占用过高
2. 根据系统的并发量合理设置worker数量，充分利用多核CPU的优势
3. 对于实时性要求较高的场景，可以适当减小处理时间间隔
4. 在处理函数中尽量避免阻塞操作，提高处理效率
