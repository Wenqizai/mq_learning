中文文档：https://www.itmuch.com/books/rocketmq/RocketMQ_Example.html

阿里云文档：https://help.aliyun.com/product/29530.html?spm=a2c4g.11186623.0.0.506c538aBXXtUy

## 概念

### Topic

第一级消息类型

### Tag

可以理解为，Topic下的第二级消息类型。服务端进行消息过滤。

### Group Name

组：代表具有相同角色的生产者组合或消费者组合在一起，称为生产者组或消费者组，分类管理的作用。

### Key

消息的业务标识，由消息生产者（Producer）设置，唯一标识某个业务逻辑，根据这些key快速检索到消息。

 一般用于消息在业务层面的唯一标识。对发送的消息设置好 Key，以后可以根据这个 Key 来查找消息。比如消息异常，消息丢失，进行查找会很方便。

`RocketMQ` 会创建专门的索引文件，用来存储 Key 与消息的映射，由于是 Hash 索引，应尽量使 Key 唯一，避免潜在的哈希冲突。Tag 和 Key 的主要差别是使用场景不同，Tag 用在 Consumer 代码中，用于服务端消息过滤Key 主要用于通过命令进行查找消息。`RocketMQ` 并不能保证 message id 唯一，在这种情况下，生产者在 push 消息的时候可以给每条消息设定唯一的 key, 消费者可以通过 message key保证对消息幂等处理。

## Quick Start

### Product Send

1. 同步消息：同步发送，可靠性高。应用如重要消息通知，短信通知；
2. 异步消息：通常应用在发送端不能容忍长时间地等待Broker响应，对响应时间敏感的业务。
3. 单向消息：主要用在不特别关心发送结果的场景，例如日志发送。

### Consumer

- 监听器：`MessageListenerConcurrently`, `MessageListenerOrderly` 继承 `MessageListener`；
- `MessageListenerConcurrently`：并发消费，`MessageListenerOrderly`：顺序消费

### 顺序消息

消息有序指的是可以按照消息的发送顺序来消费(FIFO)。`RocketMQ`可以严格的保证消息有序，可以分为**分区有序**或者**全局有序**。

- 分区有序：消息发送到多Queue
- 全局有序：消息发送到单Queue

顺序消费的原理解析，在默认的情况下消息发送会采取Round Robin轮询方式把消息发送到不同的queue(分区队列)；而消费消息的时候从多个queue上拉取消息，这种情况发送和消费是不能保证顺序。但是如果控制发送的顺序消息只依次发送到同一个queue中，消费的时候只从这个queue上依次拉取，则就保证了顺序。当发送和消费参与的queue只有一个，则是全局有序；如果多个queue参与，则为分区有序，即相对每个queue，消息都是有序的。

### 延时消息

1. 等级

delayTimeLevel：消息延迟级别，用于定时消息或消息重试。

```java
private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
```

现在RocketMq并不支持任意时间的延时，需要设置几个固定的延时等级，从1s到2h分别对应着等级1到18 消息消费失败会进入延时消息队列，消息发送时间与设置的延时等级和重试次数有关。

2. 应用

电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。

### 批量消息

批量发送消息能显著提高传递小消息的性能。限制是这些批量消息应该有相同的topic，相同的waitStoreMsgOK，而且不能是延时消息。此外，这一批消息的总大小不应超过4MB。

`waitStoreMsgOK`：消息发送时是否等消息存储完成后再返回。

### 事务消息

事务消息共有三种状态，提交状态、回滚状态、中间状态：

- TransactionStatus.CommitTransaction: 提交事务，它允许消费者消费此消息。
- TransactionStatus.RollbackTransaction: 回滚事务，它代表该消息将被删除，不允许被消费。
- TransactionStatus.Unknown: 中间状态，它代表需要检查消息队列来确定状态。





















