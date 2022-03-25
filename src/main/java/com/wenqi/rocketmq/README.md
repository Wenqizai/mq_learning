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

- **分区有序：消息发送到多Queue**
  - 用户注册需要发送验证码，以用户ID作为Sharding Key，那么同一个用户发送的消息都会按照发布的先后顺序来消费。
  - 电商的订单创建，以订单ID作为Sharding Key，那么同一个订单相关的创建订单消息、订单支付消息、订单退款消息、订单物流消息都会按照发布的先后顺序来消费。

- **全局有序：消息发送到单Queue**
  - 在证券处理中，以人民币兑换美元为Topic，在价格相同的情况下，先出价者优先处理，则可以按照FIFO的方式发布和消费全局顺序消息。


顺序消费的原理解析，在默认的情况下消息发送会采取Round Robin轮询方式把消息发送到不同的queue(分区队列)；而消费消息的时候从多个queue上拉取消息，这种情况发送和消费是不能保证顺序。但是如果控制发送的顺序消息只依次发送到同一个queue中，消费的时候只从这个queue上依次拉取，则就保证了顺序。当发送和消费参与的queue只有一个，则是全局有序；如果多个queue参与，则为分区有序，即相对每个queue，消息都是有序的。

> 常见问题

- 同一条消息是否可以既是顺序消息，又是定时消息和事务消息？

  不可以。顺序消息、定时消息、事务消息是不同的消息类型，三者是互斥关系，不能叠加在一起使用。

- 为什么全局顺序消息性能一般？

  全局顺序消息是严格按照FIFO的消息阻塞原则，即上一条消息没有被成功消费，那么下一条消息会一直被存储到Topic队列中。如果想提高全局顺序消息的TPS，可以升级实例配置，同时消息客户端应用尽量减少处理本地业务逻辑的耗时。

- 顺序消息支持哪种消息发送方式？

  顺序消息只支持可靠同步发送方式，不支持异步发送方式，否则将无法严格保证顺序。

- 顺序消息是否支持集群消费和广播消费？

  顺序消息暂时仅支持集群消费模式，不支持广播消费模式。

### 延时消息

1. 等级

delayTimeLevel：消息延迟级别，用于定时消息或消息重试。

```java
private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
```

| 延迟级别 | 时间 |
| -------- | ---- |
| 1        | 1s   |
| 2        | 5s   |
| 3        | 10s  |
| 4        | 30s  |
| 5        | 1m   |
| 6        | 2m   |
| 7        | 3m   |
| 8        | 4m   |
| 9        | 5m   |
| 10       | 6m   |
| 11       | 7m   |
| 12       | 8m   |
| 13       | 9m   |
| 14       | 10m  |
| 15       | 20m  |
| 16       | 30m  |
| 17       | 1h   |
| 18       | 2h   |

现在RocketMq并不支持任意时间的延时，需要设置几个固定的延时等级，从1s到2h分别对应着等级1到18 消息消费失败会进入延时消息队列，消息发送时间与设置的延时等级和重试次数有关。

2. 应用

电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。

### 批量消息

批量发送消息能显著提高传递小消息的性能。限制是这些批量消息应该有相同的topic，相同的waitStoreMsgOK，而且不能是延时消息。此外，这一批消息的总大小不应超过4MB。

`waitStoreMsgOK`：消息发送时是否等消息存储完成后再返回。

### 事务消息

分布式事务与传统事务:

消息队列RocketMQ版分布式事务消息不仅可以实现**应用之间的解耦**，又能保证数据的**最终一致性**。同时，传统的大事务可以被拆分为小事务，不仅能提升效率，还不会因为**某一个关联应用的不可用导致整体回滚**，从而最大限度保证核心系统的可用性。在极端情况下，如果关联的某一个应用始终无法处理成功，也只需对当前应用进行**补偿**或数据订正处理，而无需对整体业务进行回滚。

<img src="https://help-static-aliyun-doc.aliyuncs.com/assets/img/zh-CN/7087385851/p96619.png" alt="分布式事务" style="zoom: 67%;" />

事务消息共有三种状态，提交状态、回滚状态、中间状态：

- TransactionStatus.CommitTransaction: 提交事务，它允许消费者消费此消息。
- TransactionStatus.RollbackTransaction: 回滚事务，它代表该消息将被删除，不允许被消费。
- TransactionStatus.Unknown: 中间状态，暂时无法判断状态，等待固定时间以后消息队列RocketMQ版服务端根据回查规则向生产者进行消息回查。

事务过程：

![事务消息](https://help-static-aliyun-doc.aliyuncs.com/assets/img/zh-CN/3775990361/p69402.png)

> 事务回查机制说明

- 发送事务消息为什么必须要实现回查Check机制？

  当步骤1中半事务消息发送完成，但本地事务返回状态为`TransactionStatus.Unknow`，或者应用退出导致本地事务未提交任何状态时，从Broker的角度看，这条半事务消息的状态是未知的。因此Broker会定期向消息发送方即消息生产者集群中的任意一生产者实例发起消息回查，要求发送方回查该Half状态消息，并上报其最终状态。

- Check被回调时，业务逻辑都需要做些什么？

  事务消息的Check方法里面，应该写一些检查事务一致性的逻辑。消息队列RocketMQ版发送事务消息时需要实现`LocalTransactionChecker`接口，用来处理Broker主动发起的本地事务状态回查请求，因此在事务消息的Check方法中，需要完成两件事情：

  1. 检查该半事务消息对应的本地事务的状态（committed or rollback）。
  2. 向Broker提交该半事务消息本地事务的状态。

- 回查间隔

  1. 时间：系统默认每隔30秒发起一次定时任务，对未提交的半事务消息进行回查，共持续12小时。

  2. 第一次消息回查最快时间：该参数支持自定义设置。若指定消息未达到设置的最快回查时间前，系统默认每隔30秒一次的回查任务不会检查该消息。

  ```java
  Message message = new Message();
  message.putUserProperties(PropertyKeyConst.CheckImmunityTimeInSeconds, "60");
  ```

### 延时/定时消息

原生的Apache RocketMQ并不支持任意事件的延时消息和定时消息，Aliyun RocketMQ可支持定时消息。

> 概念

- 定时消息：Producer发送消息后，某一个**时间点**投递到Consumer进行消费，该消息即定时消息。
- 延时消息：Producer发送消息后，而是**延迟一定时间**后才投递到Consumer进行消费，该消息即延时消息。

> 使用场景

- 消息生产和消费有时间窗口要求，例如在电商交易中超时未支付关闭订单的场景，在订单创建时会发送一条延时消息。这条消息将会在30分钟以后投递给消费者，消费者收到此消息后需要判断对应的订单是否已完成支付。如支付未完成，则关闭订单。如已完成支付则忽略。
- 通过消息触发一些定时任务，例如在某一固定时间点向用户发送提醒消息。

> 注意事项

- 定时消息的精度会有1s~2s的延迟误差。

- 定时和延时消息的`msg.setStartDeliverTime`参数需要设置成当前时间戳之后的某个时刻（单位毫秒）。如果被设置成当前时间戳之前的某个时刻，消息将立刻投递给消费者。

- 定时和延时消息的`msg.setStartDeliverTime`参数可设置40天内的任何时刻（单位毫秒），超过40天消息发送将失败。

- `StartDeliverTime`是服务端开始向消费端投递的时间。如果消费者当前有消息堆积，那么定时和延时消息会排在堆积消息后面，将不能严格按照配置的时间进行投递。

- 由于客户端和服务端可能存在时间差，消息的实际投递时间与客户端设置的投递时间之间可能存在偏差。

- 设置定时和延时消息的投递时间后，依然受3天的消息保存时长限制。

  例如，设置定时消息5天后才能被消费，如果第5天后一直没被消费，那么这条消息将在第8天被删除。

### 消息重试(aliyun)

Consumer消费某条消息失败后，消息队列RocketMQ版会根据消息重试机制重新投递消息。若达到最大重试次数后消息还没有成功被消费，则消息将被投递至[死信队列](https://help.aliyun.com/document_detail/87277.htm#concept-2047154)。

==若自定义重试参数，请确保同一Group ID下的所有Consumer实例设置的最大重试次数和重试间隔相同，否则最后实例的配置将会覆盖前面启动的实例配置。（后面覆盖前面）==

```java
Properties properties = new Properties();
//配置对应Group ID的最大消息重试次数为20次，最大重试次数为字符串类型。
properties.put(PropertyKeyConst.MaxReconsumeTimes,"20");
//配置对应Group ID的消息重试间隔时间为3000毫秒，重试间隔时间为字符串类型。
properties.put(PropertyKeyConst.suspendTimeMillis,"3000");
Consumer consumer = ONSFactory.createConsumer(properties);
```

|   协议   | 消息类型 |                 重试间隔                  |                  最大重试次数                  |       配置方式       |
| :------: | :------: | :---------------------------------------: | :--------------------------------------------: | :------------------: |
| TCP协议  | 顺序消息 | 自定义参数：suspendTimeMillis，默认1000ms | 自定义参数：MaxReconsumeTimes，默认Integer.MAX |         代码         |
| TCP协议  | 无序消息 |        不支持自定义，间隔参看下表         |     自定义参数：MaxReconsumeTimes，默认16      |         代码         |
| HTTP协议 | 顺序消息 |                   1分钟                   |                     288次                      | 系统预设，不支持修改 |
| HTTP协议 | 无序消息 |                   5分钟                   |                     288次                      | 系统预设，不支持修改 |

| 第几次重试 | 与上次重试的间隔时间 | 第几次重试 | 与上次重试的间隔时间 |
| :--------: | :------------------: | :--------: | :------------------: |
|     1      |         10秒         |     9      |        7分钟         |
|     2      |         30秒         |     10     |        8分钟         |
|     3      |        1分钟         |     11     |        9分钟         |
|     4      |        2分钟         |     12     |        10分钟        |
|     5      |        3分钟         |     13     |        20分钟        |
|     6      |        4分钟         |     14     |        30分钟        |
|     7      |        5分钟         |     15     |        1小时         |
|     8      |        6分钟         |     16     |        2小时         |

重试次数 > 16，默认2小时。

> 注意事项

- 一条消息无论重试多少次，这些重试消息的==Message ID都不会改变==。
- 消息重试只针对集群消费模式生效；==广播消费模式不提供失败重试特性==，即消费失败后，失败消息不再重试，继续消费新的消息。

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



