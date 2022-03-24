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

`RocketMQ` 会创建专门的索引文件，用来存储 Key 与消息的映射，由于是 Hash 索引，应==务必保证key尽可能唯一，避免潜在的哈希冲突。==

Tag 和 Key 的主要差别是使用场景不同，Tag 用在 Consumer 代码中，用于服务端消息过滤Key 主要用于通过命令进行查找消息。`RocketMQ` 并不能保证 message id 唯一，在这种情况下，生产者在 push 消息的时候可以给每条消息设定唯一的 key, 消费者可以通过 message key保证对消息幂等处理。

## Quick Start

### Producer(普通消息)

1. 同步消息：同步发送，可靠性高。应用如重要消息通知，短信通知；
2. 异步消息：通常应用在发送端不能容忍长时间地等待Broker响应，对响应时间敏感的业务。
3. 单向消息：主要用在不特别关心发送结果的场景，例如日志发送。

| 发送方式 | 发送TPS | 发送结果反馈 | 可靠性   |
| -------- | ------- | ------------ | -------- |
| 同步发送 | 快      | 有           | 不丢失   |
| 异步发送 | 快      | 有           | 不丢失   |
| 单向发送 | 最快    | 无           | 可能丢失 |

### Consumer

- 监听器：`MessageListenerConcurrently`, `MessageListenerOrderly` 继承 `MessageListener`；
- `MessageListenerConcurrently`：并发消费，`MessageListenerOrderly`：顺序消费

> 消费类型

- Push：消息由消息队列RocketMQ版推送至Consumer。Push方式下，消息队列RocketMQ版还支持批量消费功能，可以将批量消息统一推送至Consumer进行消费。更多信息，请参见[批量消费](https://help.aliyun.com/document_detail/191213.htm#concept-2000662)。
- Pull：消息由Consumer主动从消息队列RocketMQ版拉取。

### 顺序消息

消息有序指的是可以按照消息的发送顺序来消费(FIFO)。`RocketMQ`可以严格的保证消息有序，可以分为**分区有序**或者**全局有序**。

要保证顺序性，需单一生产者单线程地发送消息。

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

## 实践

### 可靠信息

消息发送成功或者失败要打印消息日志，务必要打印**SendResult**和**key**字段。

#### 消息丢失情景

此时消息已经发送成功，但是消息在服务器中丢失了。

1. 没有启动Master服务器或同步刷盘，消息还没有刷盘，Broker宕机了；
2. 设置了同步刷盘，消息进入了队列中但还没有刷盘，此时Broker宕机了；
3. 设置了异步主从同步消息（ASYNC_MASTER），Master消息未同步，Master宕机了；
4. salve已经收到Master同步消息，salve设置异步刷盘，消息未刷盘，salve宕机了

#### SendStatus

- **SEND_OK**

消息发送成功，但不一定是可靠的。要确保不会丢失任何消息，还应启用同步Master服务器或同步刷盘，即SYNC_MASTER或SYNC_FLUSH。

- **FLUSH_DISK_TIMEOUT**

消息发送成功但是服务器刷盘超时。此时消息已经进入服务器队列（内存），只有服务器宕机，消息才会丢失。消息存储配置参数中可以设置刷盘方式和同步刷盘时间长度。

如果Broker服务器设置了刷盘方式为同步刷盘，即==FlushDiskType = SYNC_FLUSH==（默认为异步刷盘方式），当Broker服务器未在同步刷盘时间内（==默认为5s==）完成刷盘，则将返回该状态——刷盘超时。

- **FLUSH_SLAVE_TIMEOUT**

消息发送成功，但是服务器同步到Slave时超时。此时消息已经进入服务器队列，只有服务器宕机，消息才会丢失。

如果Broker服务器的角色是同步Master，即SYNC_MASTER（默认是异步Master即ASYNC_MASTER），并且从Broker服务器未在同步刷盘时间（默认为5秒）内完成与主服务器的同步，则将返回该状态——数据同步到Slave服务器超时。

- **SLAVE_NOT_AVAILABLE**

消息发送成功，但是此时Slave不可用。

如果Broker服务器的角色是同步Master，即SYNC_MASTER（默认是异步Master服务器即ASYNC_MASTER），但没有配置slave Broker服务器，则将返回该状态——无Slave服务器可用。

#### 消息发送失败处理

1. 至多重试2次（同步发送为2次，异步发送为0次）；
2. 如果发送失败，则轮转到下一个Broker。这个方法的总耗时时间不超过sendMsgTimeout设置的值，默认10s；
3. 如果本身向broker发送消息产生超时异常，就不会再重试。

==以上策略也是在一定程度上保证了消息可以发送成功。如果业务对消息可靠性要求比较高，建议应用增加相应的重试逻辑：比如调用send同步方法发送失败时，则尝试将消息存储到db，然后由后台线程定时重试，确保消息一定到达Broker。==

### 消息幂等

RocketMQ无法避免消息重复（Exactly-Once），所以要在业务层做幂等处理。（幂等键：msgId，key）

msgId一定是全局唯一标识符，但是实际使用中，可能会存在相同的消息有两个不同msgId的情况（消费者**主动重发**、因客户端**重投机制**导致的重复等），这种情况就需要使业务字段进行重复消费。

### 消息慢消费

- 提供消费并行度

  1. 增加Consumer 实例数量，可以通过加机器或启动多进程方式。（注意：==Consumer 实例超过订阅队列数，多余的Consumer将不能消费消息==。）
  2. 提高单个 Consumer 的消费并行线程，通过修改参数 `consumeThreadMin`、`consumeThreadMax`实现。

- 批量消费

  通过设置 consumer的`consumeMessageBatchMaxSize` 返个参数，默认是 1，即一次只消费一条消息，例如设置为 N，那么每次消费的消息数小于等于 N。

- 优化消费业务，提高消费速度

- 跳过非重要消息

​	发生消息堆积时，如果消费速度一直追不上发送速度，可选择丢不重要的消息。

```java
// 当某个队列的消息数堆积到100000条以上, 丢弃消息，直接返回success
public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
    long offset = msgs.get(0).getQueueOffset();
    String maxOffset =
        msgs.get(0).getProperty(Message.PROPERTY_MAX_OFFSET);
    long diff = Long.parseLong(maxOffset) - offset;
    if (diff > 100000) {
        // TODO 消息堆积情况的特殊处理
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
    // TODO 正常消费过程
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
}
```



