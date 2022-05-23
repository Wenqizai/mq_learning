# 文档

Apache中文文档：https://www.itmuch.com/books/rocketmq/RocketMQ_Example.html

阿里云文档：https://help.aliyun.com/product/29530.html?spm=a2c4g.11186623.0.0.506c538aBXXtUy

控制台查询消息：https://help.aliyun.com/document_detail/29540.html

RocketMQ配置解释：https://www.cnblogs.com/jice/p/11981107.html

- 一些博客

Klutzoder'Blog：https://www.klutzoder.com/RocketMQ/middleware/rocketmq-03/



![MQ对比](https://images2017.cnblogs.com/blog/178437/201711/178437-20171116111559109-292574107.png)

# 概念

### Topic

消息只能发送到同一个Topic中，同一个Topic可以分布到不同的Broker中，Broker中存储多个消费Queue。当进行消息发送时，消息客户端会拉取同一个Topic的所有Broker下所有的消费Queue进行负载均衡发送。

### Tag

可以理解为，Topic下的第二级消息类型。服务端进行消息过滤。

### Group Name

组：代表具有相同角色的生产者组合或消费者组合在一起，称为生产者组或消费者组，分类管理的作用。

### Key

消息的业务标识，由消息生产者（Producer）设置，唯一标识某个业务逻辑，根据这些key快速检索到消息。

 一般用于消息在业务层面的唯一标识。对发送的消息设置好 Key，以后可以根据这个 Key 来查找消息。比如消息异常，消息丢失，进行查找会很方便。

`RocketMQ` 会创建专门的索引文件，用来存储 Key 与消息的映射，由于是 Hash 索引，应==务必保证key尽可能唯一，避免潜在的哈希冲突。==

Tag 和 Key 的主要差别是使用场景不同，Tag 用在 Consumer 代码中，用于服务端消息过滤Key 主要用于通过命令进行查找消息。`RocketMQ` 并不能保证 message id 唯一，在这种情况下，生产者在 push 消息的时候可以给每条消息设定唯一的 key, 消费者可以通过 message key保证对消息幂等处理。

### Producer Group

生产者发送组主要用处是：若事务消息，如果某条发送某条消息的producer-A宕机，使得事务消息一直处于PREPARED状态并超时，则broker会回查同一个group的其他producer，确认这条消息应该commit还是rollback。

### Consumer Group

不同的消费组是从ConsumeQueue中拉取消息，消费消息后会记录消费的最大offset，表示之前的消息都已经消费过了，这个offset是保存再消费者组中，不是ConsumeQueue中。ConsumeQueue被消费消息后只是标记为已读状态，并不会删除消息，未删除的消息可供其他消费组消费。

这意味着，不同的group独自保存自己的消费offset，不同group的消费进度独立不相互影响。比如：a.  group 1发送积压，并不会影响到group 2的消费；b. group 1已经消费过了消息，group 2照样可以消费。

==注意：==

1. 广播模式：消息发送到所有的consumer group下的所有的consumer实例。
2. 集群模式：消息发送到所有consumer group下的其中某一个consumer实例。

Consume Queue

集群模式下同一个消费组内的消费者共同承担其订阅主题下消息队列的消费，==同一个消息消费队列在同一时刻只会被消费组内的一个消费者消费，一个消费者同一时刻可以分配多个消费队列。==

# Quick Start

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

批量消息不支持发送到Retry Group延时消息，而且这批消息的状态应该一样：`org.apache.rocketmq.common.message.MessageBatch#generateFromList`

```java
public static MessageBatch generateFromList(Collection<Message> messages) {
    assert messages != null;
    assert messages.size() > 0;
    List<Message> messageList = new ArrayList<Message>(messages.size());
    Message first = null;
    for (Message message : messages) {
        // 不支持延时消息
        if (message.getDelayTimeLevel() > 0) {
            throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
        }
        
        // 不支持重试发到重试分组
        if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            throw new UnsupportedOperationException("Retry Group is not supported for batching");
        }
        if (first == null) {
            first = message;
        } else {
            // 每条消息的Topic应该一致
            if (!first.getTopic().equals(message.getTopic())) {
                throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
            }
            // 每条消息的isWaitStoreMsgOK应该一致
            if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
            }
        }
        messageList.add(message);
    }
    MessageBatch messageBatch = new MessageBatch(messageList);

    messageBatch.setTopic(first.getTopic());
    messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
    return messageBatch;
}
```



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

(如果要支持任意精度的定时消息消费，就必须在消息服务端对消息进行排序，这势必带来很大的性能损耗。那aliyun是怎么做的？)

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

### 消息过滤

参看：https://help.aliyun.com/document_detail/29543.html

### 集群/广播消费

> 集群消费

- 集群消费模式下，每一条消息都只会被分发到一台机器上处理。如果需要被集群下的每一台机器都处理，请使用广播模式。
- 集群消费模式下，不保证每一次失败重投的消息路由到同一台机器上。

> 广播消费

- 广播消费模式下不支持顺序消息。
- 广播消费模式下不支持重置消费位点。
- 每条消息都需要被相同订阅逻辑的多台机器处理。
- 消费进度在客户端维护，出现重复消费的概率稍大于集群模式。
- 广播模式下，消息队列RocketMQ版保证每条消息至少被每台客户端消费一次，但是并不会重投消费失败的消息，因此业务方需要关注消费失败的情况。
- 广播模式下，客户端每一次重启都会从最新消息消费。客户端在被停止期间发送至服务端的消息将会被自动跳过，请谨慎选择。
- 广播模式下，每条消息都会被大量的客户端重复处理，因此推荐尽可能使用集群模式。
- 广播模式下服务端不维护消费进度，所以消息队列RocketMQ版控制台不支持消息堆积查询、消息堆积报警和订阅关系查询功能。

# 实践

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

因为不同的Message ID对应的消息内容可能相同，有可能出现冲突（重复）的情况，所以真正安全的幂等处理，不建议以Message ID作为处理依据。最好的方式是以业务唯一标识作为幂等处理的关键依据，而业务的唯一标识可以通过消息Key设置。

> 消息重复的场景

- **发送时消息重复**

  当一条消息已被成功发送到服务端并完成持久化，此时出现了网络闪断或者客户端宕机，导致服务端对客户端应答失败。 如果此时生产者意识到消息发送失败并尝试再次发送消息，消费者后续会收到两条内容相同并且Message ID也相同的消息。

- **投递时消息重复**

  消息消费的场景下，消息已投递到消费者并完成业务处理，当客户端给服务端反馈应答的时候网络闪断。为了保证消息至少被消费一次，消息队列RocketMQ版的服务端将在网络恢复后再次尝试投递之前已被处理过的消息，消费者后续会收到两条内容相同并且Message ID也相同的消息。

- **负载均衡时消息重复**（包括但不限于网络抖动、Broker重启以及消费者应用重启）

  当消息队列RocketMQ版的Broker或客户端重启、扩容或缩容时，会触发Rebalance，此时消费者可能会收到重复消息。

### 消息慢消费

> 消费耗时

- 读写外部数据库，例如MySQL数据库读写。
- 读写外部缓存等系统，例如Redis读写。
- 下游系统调用，例如Dubbo调用或者下游HTTP接口调用。

> 避免

- 梳理消息的消费耗时

  通过压测获取消息的消费耗时，并对耗时较高的操作的代码逻辑进行分析。查询消费耗时，请参见[获取消息消费耗时](https://help.aliyun.com/document_detail/193952.htm#step-zbp-czw-m7t)。梳理消息的消费耗时需要关注以下信息：

  - 消息消费逻辑的计算复杂度是否过高，代码是否存在无限循环和递归等缺陷。
  - 消息消费逻辑中的I/O操作（如：外部调用、读写存储等）是否是必须的，能否用本地缓存等方案规避。
  - 消费逻辑中的复杂耗时的操作是否可以做异步化处理，如果可以是否会造成逻辑错乱（消费完成但异步操作未完成）。

- 设置消息的消费并发度

  1. 逐步调大线程的单个节点的线程数，并观测节点的系统指标，得到单个节点最优的消费线程数和消息吞吐量。
  2. 得到单个节点的最优线程数和消息吞吐量后，根据上下游链路的流量峰值计算出需要设置的节点数，节点数=流量峰值/单线程消息吞吐量。

> 解决

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

### Topic与Tag

- **消息类型是否一致**：如普通消息、事务消息、定时（延时）消息、顺序消息，不同的消息类型使用不同的Topic，无法通过Tag进行区分。
- **业务是否相关联**：没有直接关联的消息，如淘宝交易消息，京东物流消息使用不同的Topic进行区分；而同样是天猫交易消息，电器类订单、女装类订单、化妆品类订单的消息可以用Tag进行区分。
- **消息优先级是否一致**：如同样是物流消息，盒马必须小时内送达，天猫超市24小时内送达，淘宝物流则相对会慢一些，不同优先级的消息用不同的Topic进行区分。
- **消息量级是否相当**：有些业务消息虽然量小但是实时性要求高，如果跟某些万亿量级的消息使用同一个Topic，则有可能会因为过长的等待时间而“饿死”，此时需要将不同量级的消息进行拆分，使用不同的Topic。

总的来说，针对消息分类，您可以选择创建多个Topic，或者在同一个Topic下创建多个Tag。但通常情况下，不同的Topic之间的消息没有必然的联系，而Tag则用来区分同一个Topic下相互关联的消息，例如全集和子集的关系、流程先后的关系。

### 订阅关系一致

同一个消费者Group ID下所有Consumer实例所订阅的Topic、Tag必须完全一致。如果订阅关系不一致，消息消费的逻辑就会混乱，甚至导致消息丢失。

参看：https://help.aliyun.com/document_detail/43523.htm?spm=a2c4g.11186623.0.0.5c4a180a4yjrSG

# 源码

## 问题点（待解决）

1. 消息发送时的负载均衡，多个broker，每个broker都要建Topic？

## NameServer

> 关注点

1. 服务发现与注册机制
2. 路由管理
3. 路由存储数据类型和结构
4. 单点故障与高可用

#### 注册与发现

1. Broker启动时注册，与NameServer保持长连接（长连接断了怎么处理 -> 立即删除）。NameServer每10s检测Broker是否存活；
2. 若Broker宕机，NameServer会将其在路由表中移除（并不会主动通知客户端）；
3. 消息生产者在发送消息之前先从NameServer获取Broker服务器的地址列表。

实现细节：

- Broker每隔30s向NameServer集群的每一台机器发送心跳包，包含自身创建的topic路由等信息。
- 消息客户端每隔30s向NameServer更新对应topic的路由信息。
- NameServer收到Broker发送的心跳包时会记录时间戳。
- NameServer每隔10s会扫描一次brokerLiveTable（存放心跳包的时间戳信息），如果在120s内没有收到心跳包，则认为Broker失效，更新topic的路由信息，将失效的Broker信息移除。

#### 高可用

NameServer之间互不通信，高可用是通过部署多台NameServer来实现。因此某一刻，NameServer之间的路由信息不会完全一致。但是对消息发送不会造成重大影响，只是短暂造成消息发送不均衡。

#### 源码

> 初始化

org.apache.rocketmq.namesrv.NamesrvController#initialize

```java
public boolean initialize() {
		// 加载kv
        this.kvConfigManager.load();
		// 初始化通信模块Netty
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

    	// 每10s扫描一次Broker，移除no active broker
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

    	// 每10min打印一次kv配置
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
         	// ....
        }

        return true;
    }
```

> 路由信息

org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager

```java
private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
private final ReadWriteLock lock = new ReentrantReadWriteLock();
private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
```

topicQueueTable：Topic下Queue的信息，消息发送时根据它进行负载均衡。

brokerAddrTable：Broker的基础信息，包括所属集群名称、主备Broker地址。

clusterAddrTable：Broker集群信息，存储集群中所有broker名称。

brokerLiveTable：broker状态信息，NameServer每次收到心跳包都会替换该信息。

filterServerTable：Broker上的FilterServer列表，用于类模式消息过滤。类模式过滤机制在4.4及以后版本被废弃。

> 路由注册

- broker启动向NameServer发送心跳包（循环所有的NameServer）：

org.apache.rocketmq.broker.BrokerController#start

org.apache.rocketmq.broker.out.BrokerOuterAPI#registerBrokerAll

```java
this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    @Override
    public void run() {
        try {
            BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
        } catch (Throwable e) {
            log.error("registerBrokerAll Exception", e);
        }
    }
}, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);
```

- NameServer处理心跳包

1. 收到Netty请求：

​	`org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#processRequest`

​	标识：request.getCode() = `RequestCode.REGISTER_BROKER`

2. 更新broker信息

​	org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#registerBroker

> 路由删除

1. 定时删除（定时线程池10s）

遍历 -> 时间戳比较 -> 先移除路由表信息 -> 再移除路由相关信息 -> 断开连接

org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#scanNotActiveBroker

org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#onChannelDestroy

2. 正常关闭Broker

删除该broker的路由信息

org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#unregisterBroker

3. 关闭Netty连接

org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager#onChannelDestroy

- 关于路由注册删除之间的线程问题

```markdown
> 引用：https://lists.apache.org/thread/hqclk5v2zmdq5vo6tfxtdtgw439xt8ns
这里是存在线程安全的问题。 scanNotActiveBroker 只与 unregisterBroker和 registerBroker的之间是线程不安全的。 
scanNotActiveBroker每10秒执行一次，而unregisterBroker 与 registerBroker 可能很久才会触发。甚至不会触发。 出现线程安全
的几率很低， scanNotActiveBroker 锁持有时间很长，频率高 scanNotActiveBroker 报错，可以等下下次执行, 每10秒执行一次，
那么会哟加锁，解锁的操作，比较耗时，在上面的原有下，不加锁是一种好的方式。
```

> 路由发现

路由信息的变更，NameServer不会推送到客户端，而是客户端定时拉取最新的路由信息。

标识：request.getCode() = `RequestCode.GET_ROUTEINFO_BY_TOPIC`

org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#getRouteInfoByTopic

## MQClientInstance

`JVM`中的所有消费者、生产者持有同一个`MQClientInstance`，`MQClientInstance`只会启动一次。

- 启动：`org.apache.rocketmq.client.impl.factory.MQClientInstance#start`

```java
// MQClientInstance启动方法
public void start() throws MQClientException {
  synchronized (this) {
    switch (this.serviceState) {
      case CREATE_JUST:
        this.serviceState = ServiceState.START_FAILED;
        // If not specified,looking address from name server
        if (null == this.clientConfig.getNamesrvAddr()) {
          this.mQClientAPIImpl.fetchNameServerAddr();
        }
        // Start request-response channel
        this.mQClientAPIImpl.start();
        // Start various schedule tasks
        this.startScheduledTask();
        // Start pull service (启动拉起消息线程)
        this.pullMessageService.start();
        // Start rebalance service
        this.rebalanceService.start();
        // Start push service
        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
        log.info("the client factory [{}] start OK", this.clientId);
        this.serviceState = ServiceState.RUNNING;
        break;
      case START_FAILED:
        throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
      default:
        break;
    }
  }
}
```

## Producer

图解RocketMQ消息发送和存储流程：https://cloud.tencent.com/developer/article/1717385

> 关注点

1. 消息队列的负载均衡；
2. 消息发送的高可用；
3. 批量消息发送的一致性。

#### Topic

1. 生产者每30s向NameServer同步一次路由信息；
2. NameServer中不存在Topic时，自动创建Topic

==注：Broker的路由信息是持久化的，NameServer的路由信息是在内存中。==

![](https://s2.loli.net/2022/05/21/ZBaA4bxJfTWltuI.png)

#### Send

##### 流程

> 发送高可用

1. 重试机制：默认2次；
2. 故障规避：消息发送失败后接下来的5min会将消息发送到另外的Broker。

> 消息发送流程

- commitlog

  存储消息的地方，单个文件默认1GB，文件名长度为20位，左边补零，剩余为起始偏移量。

- comsumequeue

  consumequeue作为消费消息的索引，保存指定topic下队列消息在commitlog中的其实偏移量（offset），消息大小（size）和消息Tag的哈希码。Tag过滤会用到。

<img src="https://s2.loli.net/2022/05/21/loqxWR4i3LVEdC6.png" style="zoom:50%;" />

- ReputMessageService ThreadLoop

  每休眠1ms，处理一次doReput方法。循环转发commitlog中内容到consumequeue和index文件中。

  执行方法：`org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService#doReput`

  文件分发方法：`org.apache.rocketmq.store.DefaultMessageStore#doDispatch`

- MQClientInstance

  消息客户端实例，与RocketMQ服务器（Broker，NameServer）交互，从RebalanceImpl实例的本地缓存变量topicSubscribeInfoTable中，获取该Topic主题下的消息消费队列集合（mqSet）。

  `org.apache.rocketmq.client.impl.factory.MQClientInstance#doRebalance`

- 消费组

  根据topic和ConsumerGroup参数获取该消费组下消费者id列表

  1. 广播模式所有消费端都会收到消息
  1. 集群模式消费端根据负载均衡策略获取消息（负载均衡策略：默认为平均分配算法，计算出当前Consumer端应该分配到的消息队列）


- PullMessageService ThreadLoop
  1. 获取PullRequest的处理队列ProcessQueue，然后更新该消息队列最后一次拉取的时间；`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage`
  2. 如果消费者服务状态不为`ServiceState.RUNNING`，或者当前处于暂停状态，默认延迟3s再执行`org.apache.rocketmq.client.impl.consumer.PullMessageService#executePullRequestLater`
  3. 流量控制，两个维度，消息数量达到阈值（默认1000个），或者消息体大小（默认100MB）

- ConsumeMessageService.submitConsumeRequest()

  将拉取的消息放入ProcessQueue的msgTreeMap容器中（`org.apache.rocketmq.client.impl.consumer.ConsumeMessageService#submitConsumeRequest`

- ComsumeRequest ConsumeMessageThread Pool
  1. 消费线程执行(将消息放到ProcessQueue中)`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest#run`
  1. 客户端消费消息`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#consumeMessageDirectly`，`org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently#consumeMessage`
  1. 处理消费结果`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#processConsumeResult`

- RemoteBrokerOffsetStore

  更新消费速度，发送给Broker

![send-msg](https://s2.loli.net/2022/05/21/5HPRxTKfnlVm7ws.png)

##### 发送同步消息SYNC

1. DefaultMQProducerImpl构造消息

​	构造RequestHeader，消息发送前后的钩子函数处理。

​	`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendKernelImpl`

​	`org.apache.rocketmq.client.hook.SendMessageHook`

2. 委托客户端MQClientAPIImpl处理并发送消息

​	设置请求code：`RequestCode.SEND_MESSAGE` 

​	`org.apache.rocketmq.client.impl.MQClientAPIImpl#sendMessage(java.lang.String, java.lang.String, org.apache.rocketmq.common.message.Message, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, long, org.apache.rocketmq.client.impl.CommunicationMode, org.apache.rocketmq.client.producer.SendCallback, org.apache.rocketmq.client.impl.producer.TopicPublishInfo, org.apache.rocketmq.client.impl.factory.MQClientInstance, int, org.apache.rocketmq.client.hook.SendMessageContext, org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl)`

3. Broker处理消息并返回

   - Broker接受消息请求：`org.apache.rocketmq.broker.processor.SendMessageProcessor#processRequest`

   - 校验并返回消息结果：`org.apache.rocketmq.broker.processor.SendMessageProcessor#asyncSendMessage`

```java
private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
                                                                SendMessageContext mqtraceContext,
                                                                SendMessageRequestHeader requestHeader) {
    // 1. 检查消息的核心方法
    // a. 检查Broker是否有写权限（没有创建的Topic，继承至TBW102）
    // b. 检查Topic和队列，包括权限、是否存在、是否合法等
    final RemotingCommand response = preSend(ctx, request, requestHeader);
    final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

    if (response.getCode() != -1) {
        return CompletableFuture.completedFuture(response);
    }

    final byte[] body = request.getBody();

    int queueIdInt = requestHeader.getQueueId();
    TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

    if (queueIdInt < 0) {
        queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
    }

    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setTopic(requestHeader.getTopic());
    msgInner.setQueueId(queueIdInt);

    // 如果消息重试次数超过允许的最大重试次数，消息将进入DLQ死信队列。死信队列主题为%DLQ%+消费组名。
    if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
        return CompletableFuture.completedFuture(response);
    }

    // 省略 ...
    
    CompletableFuture<PutMessageResult> putMessageResult = null;
    String transFlag = origProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
    // 事务消息处理，保存为half消息
    if (transFlag != null && Boolean.parseBoolean(transFlag)) {
        if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(
                "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending transaction message is forbidden");
            return CompletableFuture.completedFuture(response);
        }
        putMessageResult = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
    } else {
        // 非事务消息的保存，调用org.apache.rocketmq.store.MessageStore#putMessage
        putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
    }
    // 保存消息后，返回结果的处理
    return handlePutMessageResultFuture(putMessageResult, response, request, msgInner, responseHeader, mqtraceContext, ctx, queueIdInt);
}
```

##### 发送异步消息ASYNC

- 异步消息发送与同步发送逻辑大致相同，异步发送无须等待消息服务器返回本次消息发送的结果，只需要提供一个回调函数，供消息发送客户端在收到响应结果后回调。

- 异步的并发控制，通过参数clientAsyncSemaphoreValue实现，默认为65535。

- retryTimesWhenSendAsyncFailed属性来控制消息的发送重试次数，但是重试的调用入口是在收到服务端响
  应包时进行的，如果出现网络异常、网络超时等情况将不会重试。

##### 单向发送消息ONEWAY

单向消息发送客户端在收到响应结果后什么都不做了，并且没有重试机制。

##### 关于重试

- oneway没有重试，通过控制setRetryTimesWhenSendFailed和retryTimesWhenSendAsyncFailed参数来控制sync和async的重试次数。

- oneway没有重试的原因是，没有解析broker返回response，而sync和async都有解析并抛出相关异常来进行重试。包括`RemotingException`、`MQClientException`、`MQBrokerException`、`InterruptedException`

- sync和async重试处理地方不同
  - sync：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendDefaultImpl`

  ```java
  // 除了sync指定重试次数，aync和oneway都是1
  int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
  ```

  - async：`org.apache.rocketmq.client.impl.MQClientAPIImpl#sendMessageAsync`













#### DefaultMQProducer

消息生产者实现类，实现了MQAdmin接口。

```java
public class DefaultMQProducer extends ClientConfig implements MQProducer {}

public interface MQProducer extends MQAdmin {}
```

##### 启动

`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start(boolean)`

主要是启动MQClientInstance实例，MQClientInstance封装与Broker和NameServer交互的信息。

每个ClientId只有一个MQClientInstance实例（单例）。

##### send

流程

执行方法：

`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#send(org.apache.rocketmq.common.message.Message)`

执行步骤：

1. 消息长度验证：`Validators.checkMessage(msg, this.defaultMQProducer);`
2. 查找主题路由信息：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#tryToFindTopicPublishInfo`
3. 选择消息队列：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#selectOneMessageQueue`
4. 发送消息：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendKernelImpl`

注：

> 生产者查找主题路由信息：
>
> 1. 先用topic为key从本地缓存中获取，没有则向NameServer中拉取到本地路由表，再从路由表中获取。
> 2. 如果NameServer没有该Topic信息，则抛出异常MQClientException`ResponseCode.TOPIC_NOT_EXIST`
> 3. 生产者捕获异常，使用默认Topic获取路由信息（TBW102，topic信息保存在defaultMQProducer），NameServer返回路由信息（默认路由信息一定会有，主要是broker地址，队列信息）
> 4. 生产者发送消息，如果topic不存在，配置了autoCreateTopicEnable=true则发送成功，反之抛出异常：topic[" + requestHeader.getTopic() + "] not exist, apply first please!

> 选择消息队列：
>
> - 故障延迟 sendLatencyFaultEnable = false
>   1. 默认队列轮询算法（消息队列数取模），返回lastBrokerName，记录异常Broker，下次选择时可跳过，提高消息发送成功率。
> - 故障延迟 sendLatencyFaultEnable = true
>   1. 根据对消息队列进行轮询获取一个消息队列
>   2. 验证该消息队列是否可用
>   3. 如果返回MessageQueue可用，移除latencyFaultTolerance中关于该topic的条目，表明该Broker故障已经恢复
>   

Producer从NameServer中拉取到的路由信息如下图：

从图中messageQueueList可以看出，其保存的是所有的broker的队列信息，然后轮询选择队列。因此sendMessage的负载均衡是队列的轮询而不是broker下的队列轮询。`org.apache.rocketmq.client.latency.MQFaultStrategy#selectOneMessageQueue`![topicPublishInfo](https://s2.loli.net/2022/05/21/NykHYMR3ejCEPo6.png)

##### Broker故障规避机制

1. 消息发送方法

`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendDefaultImpl`

```java
// 消息发送失败重试
for (; times < timesTotal; times++) {
    // 注意这个是重试逻辑
    // 第一次循环：mq = null， 即lastBrokerName = null
    // 2次以上循环：此时mq已经被赋值了（mq = mqSelected），mq等于上一次发送失败的队列MessageQueue，此时lastBrokerName等于上次发送失败的brokerName
    // 相当于这里记录了发送失败的brokerName了
    String lastBrokerName = null == mq ? null : mq.getBrokerName();
    // 选择队列, 故障规避核心方法
    MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
    if (mqSelected != null) {
        // mq等于选择出的队列
        mq = mqSelected;
        brokersSent[times] = mq.getBrokerName();
        try {
            beginTimestampPrev = System.currentTimeMillis();
		   // 消息发送
            sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
            endTimestamp = System.currentTimeMillis();
            // 发送完毕之后, 更新Broker的信息
            // 消息发送消耗时间: endTimestamp - beginTimestampPrev
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
        }
    }
}
```

2. 记录不可用Broker的核心方法

sendLatencyFaultEnable = true时，才会调用该方法记录

`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#updateFaultItem`

```java
// 更新Broker的信息
// sendLatencyFaultEnable = true开启故障规避机制, 默认时false
// isolation = true : currentLatency = 30s
// isolation = false : currentLatency = 消息发送消耗时间 = endTimestamp - beginTimestampPrev
public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
    if (this.sendLatencyFaultEnable) {
        long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
        this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
    }
}

// 规避时间计算
// 算法: 从latencyMax数组尾部开始寻找，找到第一个比currentLatency小的下标，
// 然后从notAvailableDuration数组中获取需要规避的时长
private long computeNotAvailableDuration(final long currentLatency) {
    for (int i = latencyMax.length - 1; i >= 0; i--) {
        if (currentLatency >= latencyMax[i])
            return this.notAvailableDuration[i];
    }

    return 0;
}

// 规避策略
private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
```

3. 选择队列方法，Broker故障规避核心方法

`org.apache.rocketmq.client.latency.MQFaultStrategy#selectOneMessageQueue`

```java
// 选择队列
public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
    // 是否开启Broker故障规避
    if (this.sendLatencyFaultEnable) {
        try {
            int index = tpInfo.getSendWhichQueue().getAndIncrement();
            for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                if (pos < 0)
                    pos = 0;
                // 所有的Broker队列中，轮询选择一个队列
                MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                // 检查这个brokerName是否可用，如果broker不可用每次消息发送都会记录下来
                if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                    // 如果是第一次发送，或者broker是可用的并与上次发送失败的broker一样，则返回此broker
                    if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                        return mq;
                }
            }

            // 上面没找到broker，则尝试从规避的Broker中选择一个可用的Broker，如果没有找到，则返回null。
            final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
            // 根据notBestBroker获取该队列信息，如果broker已经断开则没有路由信息，writeQueueNums返回-1
            int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
            if (writeQueueNums > 0) {
                final MessageQueue mq = tpInfo.selectOneMessageQueue();
                if (notBestBroker != null) {
                    mq.setBrokerName(notBestBroker);
                    mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                }
                return mq;
            } else {
                latencyFaultTolerance.remove(notBestBroker);
            }
        } catch (Exception e) {
            log.error("Error occurred when selecting message queue", e);
        }

        return tpInfo.selectOneMessageQueue();
    }
	// 没开启故障规避，直接根据BrokerName选择队列
    return tpInfo.selectOneMessageQueue(lastBrokerName);
}
```

4. 执行队列选择

`org.apache.rocketmq.client.impl.producer.TopicPublishInfo#selectOneMessageQueue(java.lang.String)`

```java
public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
    // 第一次发送消息，lastBrokerName = null, 直接进行队列选择
    if (lastBrokerName == null) {
        return selectOneMessageQueue();
    } else {
        int index = this.sendWhichQueue.getAndIncrement();
        for (int i = 0; i < this.messageQueueList.size(); i++) {
            int pos = Math.abs(index++) % this.messageQueueList.size();
            if (pos < 0)
                pos = 0;
            // 所有的Broker队列中，轮询选择一个队列
            MessageQueue mq = this.messageQueueList.get(pos);
            // 判断是否是上一次发送失败的broker
            // lastBrokerName != null, lastBrokerName就代表上次发送失败的brokerName
            if (!mq.getBrokerName().equals(lastBrokerName)) {
                // 返回一个非上次失败的broker
                return mq;
            }
        }
        return selectOneMessageQueue();
    }
}

// 所有的Broker队列中，轮询选择一个队列
public MessageQueue selectOneMessageQueue() {
    int index = this.sendWhichQueue.getAndIncrement();
    int pos = Math.abs(index) % this.messageQueueList.size();
    if (pos < 0)
        pos = 0;
    return this.messageQueueList.get(pos);
}
```

5. Broker故障延迟机制核心类

`org.apache.rocketmq.client.latency.LatencyFaultTolerance`

```java
public interface LatencyFaultTolerance<T> {
    // 记录失败的Broker
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);
	// 该Broker是否可用
    // 可用判断：没有记录Broker信息或该Broker已过规避时间
    boolean isAvailable(final T name);
	// 移除失败的Broker
    void remove(final T name);
	// 尝试从规避的Broker中选择一个可用的Broker，如果没有找到，则返回null。
    T pickOneAtLeast();
}
```

> 从上述源码剖析可以看出：
>
> - 无论开启与不开启sendLatencyFaultEnable机制在消息发送时都能规避故障的Broker，见步骤4队列选择。
>
> - sendLatencyFaultEnable = true：一种较为悲观的做法。当消息发送者遇到一次消息发送失败后，就会悲观地认为Broker不可用，在接下来的一段时间内就不再向其发送消息，直接避开该Broker。
>
> - sendLatencyFaultEnable = false：只会在本次消息发送的重试过程中规避该Broker，下一次消息发送还是会继续尝试。



## Message

![image-20220407144921030](https://s2.loli.net/2022/05/21/fsv4nRyVL8Opd2P.png)

| 字段           | 用途                                 |
| -------------- | ------------------------------------ |
| flag           | RocketMQ不做处理                     |
| properties     | 用于扩展属性                         |
| tags           | 消息tag，用于消息过滤                |
| keys           | 消息索引键，用空格隔开               |
| waitStoreMsgOK | 消息发送时是否等消息储存完成后再返回 |

批量消息发送前会把消息列表压缩一遍，编码和解码的工作都是MessageDecoder来完成。



## Broker

处理发送信息请求：`org.apache.rocketmq.broker.processor.SendMessageProcessor#preSend`

消息处理，包括是否自动创建Topic：`org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#msgCheck`

- 启动

​	如果配置了autoCreateTopicEnable=true，在Broker启动流程中，会构建TopicConfigManager对象，其构造方法中首先会判断是否开启了允许自动创建主题，如果启用了自动创建主题，则向topicConfigTable中添加默认主题的路由信息。

`org.apache.rocketmq.broker.topic.TopicConfigManager#TopicConfigManager(org.apache.rocketmq.broker.BrokerController)`

## 消息存储

![消息处理流程](https://s2.loli.net/2022/05/21/i6O1RacIAqCBjmM.png)

#### 存储流程

1. 检查是否能够写入(broker slave 消息合法性)

`org.apache.rocketmq.store.DefaultMessageStore#checkStoreStatus`

`org.apache.rocketmq.store.DefaultMessageStore#checkMessage`

2. 消息的延迟级别大于0，设置延迟队列的Topic和queueId
3. 获取当前可以写入的CommitLog文件

```java
putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
try {
    MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
    long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
    this.beginTimeInLock = beginLockTimestamp;

    // Here settings are stored timestamp, in order to ensure an orderly
    // global
    msg.setStoreTimestamp(beginLockTimestamp);

    if (null == mappedFile || mappedFile.isFull()) {
        mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
    }
    if (null == mappedFile) {
        log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
    }
}
```

4. 将消息追加到文件中

`org.apache.rocketmq.store.MappedFile#appendMessagesInner`

5. 创建MsgId

```java
Supplier<String> msgIdSupplier = () -> {
    int sysflag = msgInner.getSysFlag();
    int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
    ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
    MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
    msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
    msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
    return UtilAll.bytes2string(msgIdBuffer.array());
};
```



#### 储存文件

1. `commitlog`：消息存储，所有消息主题的消息都存储再commitlog文件中；
2. `consumequeue`：消息消费队列，消息到达commitlog文件后，将异步转发到consumequeue文件中，供消息消费者消费；
3. `index`：消息索引，主要存储消息key与offset的对应关系。

三者关系图：

![关系图](https://s2.51cto.com/oss/202110/15/f630a52e3239c29e87a6622521751294.png)

==简单来说：commitlog储存消息，consumequeue绑定Topic关联的producer和consumer分发的消息，index用来查找消息。==

#### commitlog

![commitlog](https://s2.loli.net/2022/05/21/hlnHvtbKS5i86ps.png)

commitlog的消息写入是顺序写入，一旦写入不允许修改（极致利用磁盘顺序写特性），命名是以偏移量来命名，如第一个CommitLog文件为0000000000000000000，第二个CommitLog文件为00000000001073741824，依次类推。

1个commitlog文件大小是1G，第二个文件的开始偏移是1G = 1024 * 1024 * 1024B = 1073741824

##### Message ID
![msgid](https://s2.loli.net/2022/05/21/ZBRtP6eofASbMld.png)

全局唯一消息ID，共16字节。

生成：`org.apache.rocketmq.store.CommitLog.DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.store.MessageExtBrokerInner, org.apache.rocketmq.store.CommitLog.PutMessageContext)`

```java
Supplier<String> msgIdSupplier = () -> {
    int sysflag = msgInner.getSysFlag();
    int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
    ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
    MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
    msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
    msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
    return UtilAll.bytes2string(msgIdBuffer.array());
};
```

#### consumequeue

![consumequeue](![](https://s2.loli.net/2022/05/21/46Hznx1b5Xv2GoA.png)

consumequeue消息条目固定20字节，并提供index来快速定位消息条目，提高读性能。同时，由于每个消息固定20字节，就可以利用逻辑偏移计算来定位条目，无需再遍历整个consumequeue文件。

#### index
<img src="https://s2.loli.net/2022/05/21/ANwltXVobpcExP1.png" alt="commitlog"  />

> Header

- beginTimeStamp：Index文件中消息的最小存储时间

- endTimeStamp：Index文件中消息的最大存储时间
- beginPhyOffset：Index文件中存储的消息的最小物理偏移量
- endPhyOffset：Index文件中存储的消息的最大物理偏移量
- HashSlot Count：最大可存储的 hash 槽个数
- Index Count：当前已经使用的索引条目个数。注意这个值是从 1 开始

> Slot Table

该数值可通过broker.conf中`maxIndexNum`配置，储存index条目的索引。

> Index Linked List

- HashCode

  消息的 Topic 和 Message Key 经过哈希得到的整数（Topic+Message Key模糊查询，因为存在Hash冲突）。

- PhyOffset

  消息在 CommitLog 中的物理偏移量，用于到 CommitLog 中查询消息。

- timedif

  Message的落盘时间与header里的beginTimestamp的差值（精确到秒），用于根据时间范围查询消息

- pre index no

​	 hash冲突处理的关键之处，相同hash值上一个消息索引的index（如果当前消息索引是该hash值的第一个索引，则prevIndex=0, 也是消息索引查找时的停止条件）

#### checkpoint

checkpoint用来记录commitlog，consumeQueue，Index文件刷盘时间点。

![checkpoint](https://s2.loli.net/2022/05/21/eFIVvd86RHgcsrb.png)

- PhysicMsgTimestamp：commitlog文件刷盘时间点
- LogicsMsgTimestamp：consumequeue文件刷盘时间点
- IndexTimestamp：index文件刷盘时间点

#### 页缓存

`RocketMQ`引用内存映射，将磁盘文件加载到内存中，极大提升文件的读写性能。

因为`RocketMQ`操作`CommitLog`、`ConsumeQueue`文件是基于内存映射机制并在启动的时候会加载`commitlog`、`consumequeue`目录下的所有文件，所以为了避免内存与磁盘的浪费，不可能将消息永久存储在消息服务器上，这就需要引入一种机制来删除已过期的文件。(删除任务：`org.apache.rocketmq.store.DefaultMessageStore#addScheduleTask`)

```java
// 指定删除文件时间点到了
boolean timeup = this.isTimeToDelete();
// 磁盘空间满了
boolean spacefull = this.isSpaceToDelete();
// 预留手工触发机制（暂未实现）
boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;
```



#### 刷盘

![](https://klutzoder-blog.oss-cn-beijing.aliyuncs.com/2020/03/yi-bu-shua-pan-liu-cheng.png?x-oss-process=image/auto-orient,1/quality,q_90/watermark,text_a2x1dHpvZGVy,color_0c0c0c,size_20,g_se,x_10,y_10)

## Consumer

- 消费者拉取消息模式

![consumer-pull-msg](https://s2.loli.net/2022/05/21/mBbYuozKRxgM64G.png)

- 消费进度反馈机制

![消费进度反馈机制](https://s2.loli.net/2022/05/21/86KTBzUeR9Cqpjr.png)

> 关于消费进度提交机制的思考?

线程`t1`，`t2`，`t3`同时消费消息`msg1`，`msg2`，`msg3`。假设线程`t3`先于`t1`、`t2`完成处理，那么`t3`在提交消费偏移量时是提交`msg3`的偏移量吗？

如果提交`msg3`的偏移量是作为消费进度被提交，如果此时**消费端重启**，消息消费`msg1`、`msg2`就不会再被消费，这样就会造成“消息丢失”。因此`t3`线程并不会提交`msg3`的偏移量，而是**提交线程池中偏移量最小的消息的偏移量**，即`t3`线程在消费完`msg3`后，提交的消息消费进度依然是`msg1`的偏移量，这样能避免消息丢失，但同样有**消息重复消费的风险。**

#### 1. 启动流程

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#start`

第一步：构建主题订阅信息`SubscriptionData`并加入`RebalanceImpl`的订阅消息中，订阅信息的来源主要是以下两个方面。（`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#copySubscription`）

- 通过调用`DefaultMQPushConsumerImpl#subscribe（Stringtopic, String subExpression）`方法获取(此方法在构建Consumer时调用)

```java
// 实例化消费者
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group_demo_01");
// 订阅一个或者多个Topic, 以及Tag来过滤需要消费的消息
consumer.subscribe("TopicTest", "*");
```

- 订阅重试主题消息(此方法只会在集群模式下调用)

```java
case CLUSTERING:
    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),                                                                        retryTopic, SubscriptionData.SUB_ALL);
    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
    break;
```

第二步：初始化`MQClientInstance`、`RebalanceImple`（消息重新负载实现类）等

```java
this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
```

第三步：初始化消息进度。如果消息消费采用集群模式，那么消息进度存储在Broker上，如果采用广播模式，那么消息消费进度存储在消费端。

```java
 if (this.defaultMQPushConsumer.getOffsetStore() != null) {
     this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
 } else {
     switch (this.defaultMQPushConsumer.getMessageModel()) {
         case BROADCASTING:
             // 广播模式， 消息进度放在本地，即consumer端
             this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
             break;
         case CLUSTERING:
         		 // 集群模式，消息进度放在Broker
             this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
             break;
         default:
             break;
     }
     this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
 }
this.offsetStore.load();
```

第四步：创建消费端消费线程服务。`ConsumeMessageService`主要负责消息消费，在内部维护一个线程池

```java
if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
    // 顺序消费
    this.consumeOrderly = true;
    this.consumeMessageService =
        new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
} else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
    // 非顺序消费，并发消费
    this.consumeOrderly = false;
    this.consumeMessageService =
        new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
}
// 启动消费线程池
this.consumeMessageService.start();
```

第五步：向`MQClientInstance`注册消费者并启动`MQClientInstance`，`JVM`中的所有消费者、生产者持有同一个`MQClientInstance`，`MQClientInstance`只会启动一次。

```java
// mQClientFactory即MQClientInstance，只启动一次，并只有一个
boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
if (!registerOK) {
  this.serviceState = ServiceState.CREATE_JUST;
  this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
  throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                              + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                              null);
}
```

#### 2. 消息拉取

`MQClientInstance#start`启动过程中，会使用一个单独的线程`pullMessageService`进行消息的拉取。

```java
// MQClientInstance启动方法
public void start() throws MQClientException {
  synchronized (this) {
    switch (this.serviceState) {
      case CREATE_JUST:
        this.serviceState = ServiceState.START_FAILED;
        // If not specified,looking address from name server
        if (null == this.clientConfig.getNamesrvAddr()) {
          this.mQClientAPIImpl.fetchNameServerAddr();
        }
        // Start request-response channel
        this.mQClientAPIImpl.start();
        // Start various schedule tasks
        this.startScheduledTask();
        // Start pull service (启动拉起消息线程)
        this.pullMessageService.start();
        // Start rebalance service
        this.rebalanceService.start();
        // Start push service
        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
        log.info("the client factory [{}] start OK", this.clientId);
        this.serviceState = ServiceState.RUNNING;
        break;
      case START_FAILED:
        throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
      default:
        break;
    }
  }
}
```

`PullMessageService`只有在得到`PullRequest`对象时才会执行拉取任务，`PullRequest`的生成地方共有2处：

1. 一个是在`RocketMQ`根据`PullRequest`拉取任务执行完一次消息拉取任务后，又将`PullRequest`对象放入`pullRequestQueue`；
2. 另一个是在`RebalanceImpl`中创建的。

```java
public void run() {
    log.info(this.getServiceName() + " service started");
		// Stopped声明为volatile，停止正在运行线程的设计技巧
    while (!this.isStopped()) {
        try {
            // pullRequest为空，则获取阻塞
            PullRequest pullRequest = this.pullRequestQueue.take();
            // 这里会将pullRequest重新放入pullRequestQueue中，重新执行消息拉取任务
            this.pullMessage(pullRequest);
        } catch (InterruptedException ignored) {
        } catch (Exception e) {
            log.error("Pull Message Service Run Method exception", e);
        }
    }

    log.info(this.getServiceName() + " service end");
}
```

- `PullRequest`组成

```java
public class PullRequest {
  // 消费者组
  private String consumerGroup;
  // 待拉取消费队列（负载均衡）
  private MessageQueue messageQueue;
  // 消息处理队列，从Broker中拉取到的消息会先存入ProccessQueue，然后再提交到消费者消费线程池进行消费。
  private ProcessQueue processQueue;
  // 待拉取的MessageQueue偏移量
  private long nextOffset;
  // 是否被锁定
  private boolean previouslyLocked = false;
}
```

> 消息拉取基本流程

1. **客户端封装消息拉取请求**

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage`

这里会触发流控：主要的考量是担心因为一条消息堵塞，使消息进度无法向前推进，可能会造成大量消息重复消费。当触发流控时，会将`pullRequest`放到延迟队列中，延迟执行。

真正执行拉取消息的操作：`org.apache.rocketmq.client.impl.consumer.PullAPIWrapper#pullKernelImpl`

2. **消息服务器查找消息并返回**

`org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)`

```java
// 执行查找消息
// 查找条件: consumerGroup、topic、queueId
// offset(待拉取偏移量)、maMsgNums(最大拉取消息数)、messageFilter(消息过滤器)
final GetMessageResult getMessageResult =
            this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);
```

因为主从同步会存在延迟，这一步会设置下一次建议拉取的`brokerId`。如果从节点数据包含下一次拉取的偏移量，则设置下一次拉取任务建议从slave中拉取。

```java
// 是否包含下次偏移量, 是否建议从slave中拉取
long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                            * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
```


建议拉取Broker的判断条件：

1. salve是否可读  ：否 -> 从master中拉取
2. `isSuggestPullingFromSlave == true` ：当前消费较慢了，`brokerId`从配置项`whichBrokerWhenConsumeSlowly`中获取。
3. `isSuggestPullingFromSlave == false`：当前消费速度正常，使用订阅组建议的`brokerId`拉取消息进行消费，默认为master。

```java
if (this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
  // consume too slow ,redirect to another machine
  if (getMessageResult.isSuggestPullingFromSlave()) {
    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
  }
  // consume ok
  else {
    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
  }
} else {
  responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
}
```

3. **消息拉取客户端处理返回的消息**

`org.apache.rocketmq.client.impl.MQClientAPIImpl#pullMessageAsync`

`DefaultMQPushConsumerImpl$PullCallBack#onSuccess`

![消息拉取流程](https://klutzoder-blog.oss-cn-beijing.aliyuncs.com/2020/03/xiao-xi-la-qu-liu-cheng-zong-jie.png?x-oss-process=image/auto-orient,1/quality,q_90/watermark,text_a2x1dHpvZGVy,color_0c0c0c,size_20,g_se,x_10,y_10)

#### 3. 消息消费

拉取消息并提交到消费者线程池，消费消息接口：

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageService#submitConsumeRequest`

##### 3.1 并发消费

###### 消息消费

1. 提交给消费线程池

```java
 public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispatchToConsume) {
     final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
     // consumeBatchSize默认值1， msgs.size()最大值是32，受DefaultMQPushConsumer.pullBatchSize属性控制
     if (msgs.size() <= consumeBatchSize) {
         ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
         try {
             // 将consumeRequest提交到消息消费者线程池处理
             this.consumeExecutor.submit(consumeRequest);
         } catch (RejectedExecutionException e) {
             // 拒绝任务的标准处理方式: 延时重新提交
             this.submitConsumeRequestLater(consumeRequest);
         }
     } else {
         // 分批提交消费者线程池
         for (int total = 0; total < msgs.size(); ) {
             // 如果consumeBatchSize > total，则提交消费数量total = msgs.size()
            // 如果consumeBatchSize < total，则提交消费数量total = consumeBatchSize
             List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
             for (int i = 0; i < consumeBatchSize; i++, total++) {
                 if (total < msgs.size()) {
                     msgThis.add(msgs.get(total));
                 } else {
                     break;
                 }
             }

             ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
             try {
                 // 执行消费逻辑
                 this.consumeExecutor.submit(consumeRequest);
             } catch (RejectedExecutionException e) {
                 for (; total < msgs.size(); total++) {
                     msgThis.add(msgs.get(total));
                 }

                 this.submitConsumeRequestLater(consumeRequest);
             }
         }
     }
 }
```

2. 消费者线程池处理消息

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest#run`

```java
@Override
public void run() {
  // 队列已drop, 停止消费
  if (this.processQueue.isDropped()) {
    return;
  }

  MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
  ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
  ConsumeConcurrentlyStatus status = null;
  // 如果消息来自延迟队列则设置其主题为%RETRY_TOPIC%+consumerGroup
  defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

  ConsumeMessageContext consumeMessageContext = null;
  // 执行钩子函数
  if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
    consumeMessageContext = new ConsumeMessageContext();
    consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
    consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
    consumeMessageContext.setProps(new HashMap<String, String>());
    consumeMessageContext.setMq(messageQueue);
    consumeMessageContext.setMsgList(msgs);
    consumeMessageContext.setSuccess(false);
    ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
  }

  long beginTimestamp = System.currentTimeMillis();
  boolean hasException = false;
  ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
  try {
    if (msgs != null && !msgs.isEmpty()) {
      for (MessageExt msg : msgs) {
        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
      }
    }
    // 执行具体消费, 调用listener方法
    status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
  } catch (Throwable e) {
    log.warn(String.format("consumeMessage exception: %s Group: %s Msgs: %s MQ: %s",
                           RemotingHelper.exceptionSimpleDesc(e),
                           ConsumeMessageConcurrentlyService.this.consumerGroup,
                           msgs,
                           messageQueue), e);
    hasException = true;
  }
  long consumeRT = System.currentTimeMillis() - beginTimestamp;
  // 消费失败
  if (null == status) {
    if (hasException) {
      returnType = ConsumeReturnType.EXCEPTION;
    } else {
      returnType = ConsumeReturnType.RETURNNULL;
    }
  // 客户端15分钟没有返回消费结果认定为超时
  } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
    returnType = ConsumeReturnType.TIME_OUT;
  } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
    returnType = ConsumeReturnType.FAILED;
  } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
    returnType = ConsumeReturnType.SUCCESS;
  }

 
  if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
    consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
  }

  if (null == status) {
    log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
             ConsumeMessageConcurrentlyService.this.consumerGroup,
             msgs,
             messageQueue);
    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
  }

   // 执行钩子函数
  if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
    consumeMessageContext.setStatus(status.toString());
    consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
    ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
  }

  ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
    .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

  if (!processQueue.isDropped()) {
    // 队列没有被删除, 处理消费结果
    // 如果因新的消费者加入或原先的消费者出现宕机，导致原先分配给消费者的队列在负载之后分配给了别的消费者，那么消息会被重复消费)
    ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
  } else {
    log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
  }
}
```

3. 处理消费结果

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#processConsumeResult`

```java
public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

  			// ack计算
        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
            		//  广播模式: 业务方会返回RECONSUME_LATER，消息并不会被重新消费，而是以警告级别输出到日志文件中
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
               // 集群模式: 执行sendMessageBack发送ack
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                // 消费状态是RECONSUME_LATER才会走到这里
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);
				   // ack发送失败延迟执行
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }
		// 从ProcessQueue中移除这批消息
  		// 这里返回移除消费消息后队列的最小偏移量
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            // 用最小偏移量offset来更新消费进度
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }
```

> 从`ProcessQueue`中移除这批消息，这里返回的偏移量是移除该批消息后最小的偏移量。然后用该偏移量更新消息消费进度，以便消费者重启后能从上一次的消费进度开始消费，避免消息重复消费。值得注意的是，当消息监听器返回`RECONSUME_LATER`时，消息消费进度也会向前推进，并用`ProcessQueue`中最小的队列偏移量调用消息消费进度存储器`OffsetStore`更新消费进度。这是因为当返回`RECONSUME_LATER`时，`RocketMQ`会创建一条与原消息属性相同的消息，拥有一个唯一的新`msgId`，并存储原消息ID，该消息会存入`CommitLog`文件，与原消息没有任何关联，所以该消息也会进入`ConsuemeQueue`，并拥有一个全新的队列偏移量。

```txt
# 发送的消息
SendResult [sendStatus=SEND_OK, msgId=7F00000175CC18B4AAC259C3E2040000, offsetMsgId=0A00593B00002A9F000000000003AC51, messageQueue=MessageQueue [topic=TopicTest, brokerName=broker-a, queueId=3], queueOffset=120]

# 消费状态返回RECONSUME_LATER，重新消费的消息，可以看到源消息的属性放在Message.properties里面
ConsumeMessageThread_source-consumer-quick-start_1 Receive New Messages: [MessageExt [brokerName=broker-a, queueId=3, storeSize=198, queueOffset=120, sysFlag=0, bornTimestamp=1652840409604, bornHost=/10.0.89.59:61748, storeTimestamp=1652840409609, storeHost=/10.0.89.59:10911, msgId=0A00593B00002A9F000000000003AC51, commitLogOffset=240721, bodyCRC=1446496313, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=121, CONSUME_START_TIME=1652840409617, UNIQ_KEY=7F00000175CC18B4AAC259C3E2040000, CLUSTER=DefaultCluster, TAGS=TagA}, body=[83, 111, 117, 114, 99, 101, 32, 58, 32, 72, 101, 108, 108, 111, 32, 82, 111, 99, 107, 101, 116, 77, 81, 48], transactionId='null'}]] 
ConsumeMessageThread_source-consumer-quick-start_2 Receive New Messages: [MessageExt [brokerName=broker-a, queueId=0, storeSize=372, queueOffset=42, sysFlag=0, bornTimestamp=1652840409604, bornHost=/10.0.89.59:61748, storeTimestamp=1652840491965, storeHost=/10.0.89.59:10911, msgId=0A00593B00002A9F000000000003AE7C, commitLogOffset=241276, bodyCRC=1446496313, reconsumeTimes=1, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MIN_OFFSET=0, REAL_TOPIC=%RETRY%source-consumer-quick-start, ORIGIN_MESSAGE_ID=0A00593B00002A9F000000000003AC51, RETRY_TOPIC=TopicTest, MAX_OFFSET=43, CONSUME_START_TIME=1652840492137, UNIQ_KEY=7F00000175CC18B4AAC259C3E2040000, CLUSTER=DefaultCluster, WAIT=false, DELAY=3, TAGS=TagA, REAL_QID=0}, body=[83, 111, 117, 114, 99, 101, 32, 58, 32, 72, 101, 108, 108, 111, 32, 82, 111, 99, 107, 101, 116, 77, 81, 48], transactionId='null'}]] 
ConsumeMessageThread_source-consumer-quick-start_3 Receive New Messages: [MessageExt [brokerName=broker-a, queueId=0, storeSize=372, queueOffset=43, sysFlag=0, bornTimestamp=1652840409604, bornHost=/10.0.89.59:61748, storeTimestamp=1652840522157, storeHost=/10.0.89.59:10911, msgId=0A00593B00002A9F000000000003B155, commitLogOffset=242005, bodyCRC=1446496313, reconsumeTimes=2, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MIN_OFFSET=0, REAL_TOPIC=%RETRY%source-consumer-quick-start, ORIGIN_MESSAGE_ID=0A00593B00002A9F000000000003AC51, RETRY_TOPIC=TopicTest, MAX_OFFSET=44, CONSUME_START_TIME=1652840522159, UNIQ_KEY=7F00000175CC18B4AAC259C3E2040000, CLUSTER=DefaultCluster, WAIT=false, DELAY=4, TAGS=TagA, REAL_QID=0}, body=[83, 111, 117, 114, 99, 101, 32, 58, 32, 72, 101, 108, 108, 111, 32, 82, 111, 99, 107, 101, 116, 77, 81, 48], transactionId='null'}]] 
ConsumeMessageThread_source-consumer-quick-start_4 Receive New Messages: [MessageExt [brokerName=broker-a, queueId=0, storeSize=372, queueOffset=44, sysFlag=0, bornTimestamp=1652840409604, bornHost=/10.0.89.59:61748, storeTimestamp=1652840582237, storeHost=/10.0.89.59:10911, msgId=0A00593B00002A9F000000000003B42E, commitLogOffset=242734, bodyCRC=1446496313, reconsumeTimes=3, preparedTransactionOffset=0, toString()=Message{topic='TopicTest', flag=0, properties={MIN_OFFSET=0, REAL_TOPIC=%RETRY%source-consumer-quick-start, ORIGIN_MESSAGE_ID=0A00593B00002A9F000000000003AC51, RETRY_TOPIC=TopicTest, MAX_OFFSET=45, CONSUME_START_TIME=1652840582241, UNIQ_KEY=7F00000175CC18B4AAC259C3E2040000, CLUSTER=DefaultCluster, WAIT=false, DELAY=5, TAGS=TagA, REAL_QID=0}, body=[83, 111, 117, 114, 99, 101, 32, 58, 32, 72, 101, 108, 108, 111, 32, 82, 111, 99, 107, 101, 116, 77, 81, 48], transactionId='null'}]] 

```

###### 消息确认

如果消息监听器返回的消费结果为`RECONSUME_LATER`，则需要将这些消息发送给Broker来延迟消息。`org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#sendMessageBack`

> consumer发送过程

`org.apache.rocketmq.client.impl.MQClientAPIImpl#consumerSendMessageBack`

```java
public void consumerSendMessageBack(final String addr, final MessageExt msg, final String consumerGroup, final int delayLevel, final long timeoutMillis, final int maxConsumeRetryTimes) throws RemotingException, MQBrokerException, InterruptedException {
    ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
    // 请求码：RequestCode.CONSUMER_SEND_MSG_BACK
    RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
	// 将源消息的属性设置到requestHeader中
    requestHeader.setGroup(consumerGroup);
    requestHeader.setOriginTopic(msg.getTopic());
    requestHeader.setOffset(msg.getCommitLogOffset());
    requestHeader.setDelayLevel(delayLevel);
    requestHeader.setOriginMsgId(msg.getMsgId());
    requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
	// 同步发送到Broker
    RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                                                              request, timeoutMillis);
    assert response != null;
    switch (response.getCode()) {
        case ResponseCode.SUCCESS: {
            return;
        }
        default:
            break;
    }
	// 注意：这里返回的错误，上一层方法有兜底逻辑，会创建一条新消息发送到重试队列里面
    throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
}
```

> broker处理过程

`org.apache.rocketmq.broker.processor.SendMessageProcessor#asyncProcessRequest(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)`

`org.apache.rocketmq.broker.processor.SendMessageProcessor#asyncConsumerSendMsgBack`

```java
private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
    // ...
    
    // 根据group来获取订阅配置信息
    SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
    // 注意：一下检验返回的错误，在consumer有兜底逻辑，会创建一条新消息发送到重试队列里面 
    // 如果配置信息为空，而且不能自动创建配置，返回配置组信息不存在错误
    if (null == subscriptionGroupConfig) {
        response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
        response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                           + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
        return CompletableFuture.completedFuture(response);
    }
    // broker不可写(消息发给重试队列)
    if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
        response.setCode(ResponseCode.NO_PERMISSION);
        response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
        return CompletableFuture.completedFuture(response);
    }
	// 重试队列为0，不可重试，直接返回ResponseCode.SUCCESS，把消息给吞掉
    if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return CompletableFuture.completedFuture(response);
    }
	// 创建可读写的重试队列：topic：%RETRY%+消费组名称，队列数：默认1
    String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
    int queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % subscriptionGroupConfig.getRetryQueueNums();
    int topicSysFlag = 0;
    if (requestHeader.isUnitMode()) {
        topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
    }

    TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
        newTopic,
        subscriptionGroupConfig.getRetryQueueNums(),
        PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
    if (null == topicConfig) {
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("topic[" + newTopic + "] not exist");
        return CompletableFuture.completedFuture(response);
    }

    if (!PermName.isWriteable(topicConfig.getPerm())) {
        response.setCode(ResponseCode.NO_PERMISSION);
        response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
        return CompletableFuture.completedFuture(response);
    }
    // 根据偏移量从commitLog中读取消息
    MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
    if (null == msgExt) {
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("look message by offset failed, " + requestHeader.getOffset());
        return CompletableFuture.completedFuture(response);
    }
	// 设置消息msgExt的属性
    final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
    if (null == retryTopic) {
        MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
    }
    msgExt.setWaitStoreMsgOK(false);

    int delayLevel = requestHeader.getDelayLevel();
	// 获取最大重试次数
    int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
    if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
        Integer times = requestHeader.getMaxReconsumeTimes();
        if (times != null) {
            maxReconsumeTimes = times;
        }
    }
	// 超过最大重试次数，设置topic：%DLQ%+消费组名称，意味着该消息会进入死信队列
    // 进入DLQ队列，RocketMQ将不负责再次调度消费了，需要人工干预
    if (msgExt.getReconsumeTimes() >= maxReconsumeTimes
        || delayLevel < 0) {
        newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
        queueIdInt = ThreadLocalRandom.current().nextInt(99999999) % DLQ_NUMS_PER_GROUP;

        topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return CompletableFuture.completedFuture(response);
        }
        msgExt.setDelayTimeLevel(0);
    } else {
        if (0 == delayLevel) {
            delayLevel = 3 + msgExt.getReconsumeTimes();
        }
        msgExt.setDelayTimeLevel(delayLevel);
    }

    // 创建一条新的消息储存到commitlog，属性内容与源消息保持一致
    MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
    msgInner.setTopic(newTopic);
    msgInner.setBody(msgExt.getBody());
    msgInner.setFlag(msgExt.getFlag());
    MessageAccessor.setProperties(msgInner, msgExt.getProperties());
    msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
    msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

    msgInner.setQueueId(queueIdInt);
    msgInner.setSysFlag(msgExt.getSysFlag());
    msgInner.setBornTimestamp(msgExt.getBornTimestamp());
    msgInner.setBornHost(msgExt.getBornHost());
    msgInner.setStoreHost(msgExt.getStoreHost());
    msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

    String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
    MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
    msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
	// 异步保存到commitlog
    CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
    return putMessageResult.thenApply((r) -> {
        if (r != null) {
            switch (r.getPutMessageStatus()) {
                case PUT_OK:
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }
                    /**
                     * 在存入CommitLog文件之前，如果消息的延迟级别delayTimeLevel大于0，
                     * 将消息的主题与队列替换为定时任务主题“SCHEDULE_TOPIC_XXXX”，队列ID为延迟级别减1。
                     * 再次将消息主题、队列存入消息属性，键分别为PROPERTY_REAL_TOPIC、PROPERTY_REAL_QUEUE_ID。
                     */
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msgInner.getTopic())) {
                        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), r.getAppendMessageResult().getWroteBytes());
                        this.brokerController.getBrokerStatsManager().incQueuePutNums(msgInner.getTopic(), msgInner.getQueueId());
                        this.brokerController.getBrokerStatsManager().incQueuePutSize(msgInner.getTopic(), msgInner.getQueueId(), r.getAppendMessageResult().getWroteBytes());
                    }
                    this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    return response;
                default:
                    break;
            }
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(r.getPutMessageStatus().name());
            return response;
        }
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("putMessageResult is null");
        return response;
    });
}
```

##### 3.2 消费进度

广播模式：同一Consume Group下的所有consumer都需要消费消息。因此消费进度应该是每个consumer独立保存的。

集群模式：同一Consume Group下，一条消息只会分配给一个consumer。因此消费进度对于每个的consumer都是可见，可访问的。

> 广播模式

广播模式消费进度保存地方：`org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore`

> 集群模式

集群模式消费进度保存方法：`org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore`。消费进度保存地方：consumer内存，Broker内存和磁盘。

集群模式本地保存一份进度`offsetTable`，持久化是通过网络同步到Broker的内存中，然后由Broker负责定时持久化（持久化规则：topic@group，相当于每个group都独立保存一份进度）。Broker处理消费进度地方：`org.apache.rocketmq.broker.offset.ConsumerOffsetManager#commitOffset(java.lang.String, java.lang.String, java.lang.String, int, long)`

#### 4. 负载均衡

我们知道consumer从`pullRequestQueue`中获取`PullRequest`对象，进行消息的拉取和消费。这意味者`PullRequest`对象的创建放入队列时，已经完成的负载均衡的操作。所以若要搞清楚消费者的负载均衡机制，就从`PullRequest`对象创建说起。

1. `MQClientInstance`启动过程中，会`new RebalanceService()`负责consumer的负载均衡的线程，每20s运行一遍。（`org.apache.rocketmq.client.impl.consumer.RebalanceService#run`）

```java
@Override
public void run() {
  log.info(this.getServiceName() + " service started");

  while (!this.isStopped()) {
    // 等待20s，可配置
    this.waitForRunning(waitInterval);
    // 执行负载均衡操作
    this.mqClientFactory.doRebalance();
  }

  log.info(this.getServiceName() + " service end");
}
```

2. 遍历已注册到`MQClientInstance`中消费者对象（`consumerTable`），并执行负载均衡

```java
public void doRebalance() {
  for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
    MQConsumerInner impl = entry.getValue();
    if (impl != null) {
      try {
        impl.doRebalance();
      } catch (Throwable e) {
        log.error("doRebalance exception", e);
      }
    }
  }
}
```

3. 根据Topic获取所有的订阅信息。`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#doRebalance`

```java
public void doRebalance(final boolean isOrder) {
  // 获取订阅信息
  // 订阅信息在调用消费者DefaultMQPushConsumerImpl#subscribe方法时填充
  Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
  if (subTable != null) {
    for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
      final String topic = entry.getKey();
      try {
        // 负载均衡的核心方法
        this.rebalanceByTopic(topic, isOrder);
      } catch (Throwable e) {
        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
          log.warn("rebalanceByTopic Exception", e);
        }
      }
    }
  }
  // 查看DefaultMQPushConsumerImpl#unsubscribe发现：取消订阅时只是移除subTable的信息，而processQueueTable的信息并没移除
  // 这个方法就是执行就是将不关心的主题消费队列从processQueueTable中移除
  this.truncateMessageQueueNotMyTopic();
}
```

4. 从主题订阅信息缓存表中获取主题的队列信息（`mqSet`），同时从Broker中获取该消费组内当前所有的消费者客户端ID（`cidAll`）。执行负载均衡之前对两者进行排序。`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#rebalanceByTopic`

```java
private void rebalanceByTopic(final String topic, final boolean isOrder) {
  switch (messageModel) {
    case BROADCASTING: {
      // 省略广播模式（注：广播模式只需队列均衡，所有cid都会发一份）
    case CLUSTERING: {
      // 主题订阅信息缓存表中获取主题的队列信息
      Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
      // find by consumer group, 获取本消费者组下的所有消费者client id
      List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
     	// 同时不为空才进行负载均衡
      if (mqSet != null && cidAll != null) {
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        mqAll.addAll(mqSet);
			  // 先排序，同一个消费组内看到的视图应保持一致，确保同一个消费队列不会被多个消费者分配
        Collections.sort(mqAll);
        Collections.sort(cidAll);
        
        // 省略执行均衡策略过程...
        
        Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
        if (allocateResult != null) {
          allocateResultSet.addAll(allocateResult);
        }

        // 这个方法用来检查Broker的队列是否发生了变化，用当前负载队列allocateResultSet进行对比
        boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
        if (changed) {
          // 发生了变化，更新
          this.messageQueueChanged(topic, mqSet, allocateResultSet);
        }
      }
      break;
    }
    default:
      break;
  }
```

5. 移除负载均衡队列中的无用的队列，将新的队列创建一个新的`PullRequest`加入到消息拉取线程中`PullMessageService`。`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#updateProcessQueueTableInRebalance`

![consumer的负载均衡](https://s2.loli.net/2022/05/21/dMvFqprKl6EtnbI.png)

整体流程图

![](https://s2.loli.net/2022/05/21/bBm6nqrRIXx3DVZ.png)

6. 在进行消息负载时，如果消息消费队列被分配给其他消费者，会将该`ProcessQueue`状态设置为`droped`，持久化该消息队列的消费进度，并从内存中将其移除。

   `org.apache.rocketmq.client.impl.consumer.RebalanceImpl#removeUnnecessaryMessageQueue`

```java
if (mq.getTopic().equals(topic)) {
    if (!mqSet.contains(mq)) {
        pq.setDropped(true);
        // 持久化消费进度，并移除内存的消费进度（集群模式：本地， Broker内存和磁盘都会保存一份）
        if (this.removeUnnecessaryMessageQueue(mq, pq)) {
            it.remove();
            changed = true;
            log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
        }
    } else if (pq.isPullExpired()) {
        switch (this.consumeType()) {
            case CONSUME_ACTIVELY:
                break;
            case CONSUME_PASSIVELY:
                pq.setDropped(true);
                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    it.remove();
                    changed = true;
                    log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                              consumerGroup, mq);
                }
                break;
            default:
                break;
        }
    }
}
```


> 消息负载算法如果没有特殊的要求，尽量使用`AllocateMessageQueueAveragely`、`AllocateMessageQueueAveragelyByCircle`，这是因为分配算法比较直观。
>
> 消息队列分配原则为一个消费者可以分配多个消息队列，但同一个消息队列只会分配给一个消费者，故如果消费者个数大于消息队列数量，则有些消费者无法消费消息。
>
> 同一个消息队列只会分配给一个消费者：通过对队列和消费者实例id排序实现，保证每个消费者实例看到的分配的视图都是一致的，所以分配时才能保证同一个队列分配给同一个消费者。假如consumer 1被consumer 2分配了新的队列Entry 5和Entry 6，consumer 1会在负载均衡最后对比一下已负载均衡分配的队列和同步的队列，发现有新的队列就创建`pullRequest`提交到`pullRequestQueue`。

#### 5. 消息拉取与消费进度

消费拉取线程定时从broker拉取消息，保存到本地`ProcessQueue`中，存储结构`TreeMap<Long /*偏移量*/, MessageExt /*消息*/> msgTreeMap = new TreeMap<Long, MessageExt>();`。然后提交到消费者线程池处理。消费者线程池每处理完一个消息消费任务（`ConsumeRequest`），会从`ProcessQueue`中移除本批消费的消息，并返回`ProcessQueue`中最小的偏移量，用该偏移量更新消息队列消费进度，也就是说==更新消费进度与消费任务中的消息没有关系==。

- 例如：

现在有两个消费任务`task1`（`queueOffset`分别为20、40）和`task2`（`queueOffset`分别为50、70），并且`ProcessQueue`中当前包含最小消息偏移量为10的消息。

1. `task2`消费结束后，将使用10更新消费进度，而不是70。
2. `task1`消费结束后，还是以10更新消息队列消费进度。

==消息消费进度的推进取决于`ProcessQueue`中偏移量最小的消息消费速度。==如果偏移量为10的消息消费成功，且`ProcessQueue`中包含消息偏移量为100的消息，则消息偏移量为10的消息消费成功后，将直接用100更新消息消费进度。

- 问题

如果在消费消息偏移量为10的消息时发生了死锁，会导致消息一直无法被消费，岂不是消息进度无法向前推进了？

是的，为了避免这种情况，`RocketMQ`引入了一种消息拉取流控措施：`DefaultMQPushConsumer#consumeConcurrentlyMaxSpan = 2000`，消息处理队列`ProcessQueue`中最大消息偏移与最小偏移量不能超过该值，如果超过该值，将触发流控，延迟该消息队列的消息拉取。

```java
public long removeMessage(final List<MessageExt> msgs) {
    // 初始化消费进度 -1
    long result = -1;
    final long now = System.currentTimeMillis();
    try {
        this.treeMapLock.writeLock().lockInterruptibly();
        this.lastConsumeTimestamp = now;
        try {
            if (!msgTreeMap.isEmpty()) {
                // 本地消息不为空，消费进度为队列最大偏移量
                result = this.queueOffsetMax + 1;
                int removedCnt = 0;
                for (MessageExt msg : msgs) {
                    // 从msgTreeMap中移除改消息，
                    MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                    if (prev != null) {
                        removedCnt--;
                        msgSize.addAndGet(0 - msg.getBody().length);
                    }
                }
                msgCount.addAndGet(removedCnt);

                if (!msgTreeMap.isEmpty()) {
                    // 移除已经消费消息之后，本地消息不为空，返回消费进度为最小偏移量
                    result = msgTreeMap.firstKey();
                }
            }
        } finally {
            this.treeMapLock.writeLock().unlock();
        }
    } catch (Throwable t) {
        log.error("removeMessage exception", t);
    }

    return result;
}
```

#### 6. 定时消息

定时消息实现类：`ScheduleMessageService`

1. 加载 load()

`org.apache.rocketmq.store.schedule.ScheduleMessageService#load`

该方法主要完成延迟消息**消费队列消息进度的加载**与**`delayLevelTable`数据的构造**，延迟队列消息消费进度默认存储路径为`${ROCKET_HOME}/store/config/delayOffset.json`。

2. 启动 start()

Broker启动时触发此方法：`org.apache.rocketmq.store.schedule.ScheduleMessageService#start`

该方法主要是遍历所有的延时级别，创建定时任务线程池，并执行定时任务。第一次启动时，默认延迟1 s后执行一次定时任务，从第二次调度开始，才使用相应的延迟时间执行定时任务。

**设计关键点：**

1. 定时消息单独一个主题：`SCHEDULE_TOPIC_XXXX`，该主题下的队列数量等于`MessageStoreConfig#messageDelayLevel`配置的延迟级别，其对应关系为`queueId`等于延迟级别减1。(`queueId = delayLevel - 1`)
2. `ScheduleMessageService`为每个延迟级别创建一个定时器，根据延迟级别对应的延迟时间进行延迟调度。
3. 在消息发送时，如果消息的延迟级别`delayLevel`大于0，将消息的原主题名称、队列ID存入消息属性，然后改变消息的主题、队列与延迟主题所属队列，消息将最终转发到延迟队列的消费队列中。

```java
public void start() {
    if (started.compareAndSet(false, true)) {
        super.load();
        this.deliverExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
        if (this.enableAsyncDeliver) {
            this.handleExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
        }
        // 遍历所有延时级别
        // ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(32);
        for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
            Integer level = entry.getKey();
            Long timeDelay = entry.getValue();
            // offsetTable 保存消费进度
            // ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }
		   // 创建定时任务
            if (timeDelay != null) {
                if (this.enableAsyncDeliver) {
                    this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
                // 执行延时任务，时间信息保存在DeliverDelayedMessageTimerTask
                this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
            }
        }
	    // 每10s持久化一次线程池，通过flushDelayOffsetInterval配置
        this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (started.get()) {
                        ScheduleMessageService.this.persist();
                    }
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
    }
}
```

3. 定时调度逻辑

定时调度的核心类：`DeliverDelayedMessageTimerTask`

`org.apache.rocketmq.store.schedule.ScheduleMessageService.DeliverDelayedMessageTimerTask#executeOnTimeup`

**设计关键点：**

消息存储时，如果消息的延迟级别属性`delayLevel > 0`，则会备份原主题、原队列到消息属性中，其键分别为PROPERTY_REAL_TOPIC、PROPERTY_REAL_QUEUE_ID，通过为不同的延迟级别创建不同的调度任务，到达延迟时间后执行调度任务。

调度任务主要是根据延迟拉取消息消费进度从延迟队列中拉取消息，然后从`CommitLog`文件中加载完整消息，清除延迟级别属性并恢复原先的主题、队列，再次创建一条新的消息存入`CommitLog`文件并转发到消息消费队列中供消息消费者消费。

```java
public void executeOnTimeup() {
    // 根据delayLevel查找对应的消费队列
    // 如果未找到，说明当前不存在该延时级别的消息，则忽略本次任务，根据延时级别创建下一次调度任务
    ConsumeQueue cq = ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));
    if (cq == null) {
        this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
        return;
    }

    // 根据offset从消费队列cq中，获取消息
    // 如果未找到，则更新延迟队列的定时任务消费进度resetOffset，根据延时级别创建下一次调度任务
    SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
    if (bufferCQ == null) {
        long resetOffset;
        if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
            log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}", this.offset, resetOffset, cq.getQueueId());
        } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
            log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}", this.offset, resetOffset, cq.getQueueId());
        } else {
            resetOffset = this.offset;
        }
        this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
        return;
    }

    long nextOffset = this.offset;
    try {
        int i = 0;
        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
        for (; i < bufferCQ.getSize() && isStarted(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
            // 遍历ConsumeQueue文件，每一个标准ConsumeQueue条目为20个字节。
            // 解析出消息的物理偏移量、消息长度、消息标志的哈希码，为从CommitLog文件加载具体的消息做准备
            long offsetPy = bufferCQ.getByteBuffer().getLong();
            int sizePy = bufferCQ.getByteBuffer().getInt();
            long tagsCode = bufferCQ.getByteBuffer().getLong();
            if (cq.isExtAddr(tagsCode)) {
                if (cq.getExt(tagsCode, cqExtUnit)) {
                    tagsCode = cqExtUnit.getTagsCode();
                } else {
                    //can't find ext content.So re compute tags code.
                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}", tagsCode, offsetPy, sizePy);
                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                }
            }
            long now = System.currentTimeMillis();
            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            long countdown = deliverTimestamp - now;
            if (countdown > 0) {
                this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                return;
            }

            // 根据消息物理偏移量与消息大小从CommitLog文件中查找消息。
            MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
            if (msgExt == null) {
                continue;
            }
            
		   // 根据消息属性重新构建新的消息对象，清除消息的延迟级别属性（delayLevel）
            // 恢复消息原先的消息主题与消息消费队列，消息的消费次数reconsumeTimes并不会丢失
            MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
            if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}", msgInner.getTopic(), msgInner);
                continue;
            }

            // 将消息再次存入CommitLog文件，并转发到主题对应的消息队列上，供消费者再次消费。
            // 存储成功后，更新延迟队列的拉取进度。
            boolean deliverSuc;
            if (ScheduleMessageService.this.enableAsyncDeliver) {
                deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
            } else {
                deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
            }

            if (!deliverSuc) {
                this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                return;
            }
        }

        nextOffset = this.offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
    } catch (Exception e) {
        log.error("ScheduleMessageService, messageTimeup execute error, offset = {}", nextOffset, e);
    } finally {
        bufferCQ.release();
    }

    this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
}
```

###### 6.1 完整流程

<img src="https://s2.loli.net/2022/05/21/4H5jGWFyxSvPRKD.png" alt="schedule-msg" style="zoom: 80%;" />

1. 发送消息的`delayLevel`大于0，则将消息主题变更为`SCHEDULE_TOPIC_XXXX`，消息队列为`delayLevel`减1。（`org.apache.rocketmq.store.CommitLog#asyncPutMessage`）；
2. 消息经由`CommitLog`文件转发到消息队列`SCHEDULE_TOPIC_XXXX`中；（根据Topic转发到每一个consume queue：`org.apache.rocketmq.store.DefaultMessageStore.CommitLogDispatcherBuildConsumeQueue#dispatch`）
3. 定时任务Time每隔`1s`根据上次拉取偏移量从消费队列中取出所有消息；
4. 根据消息的物理偏移量与消息大小从`CommitLog`文件中拉取消息。
5. 根据消息属性重新创建消息，恢复原主题`topicA`、原队列ID，清除`delayLevel`属性，并存入`CommitLog`文件。
6. 将消息转发到原主题`topicA`的消息消费队列，供消息消费者消费。

#### 7. 消息过滤

![consumequeue](https://s2.loli.net/2022/05/21/46Hznx1b5Xv2GoA.png)

> 过滤机制

`RocketMQ`消息过滤方式是在订阅时进行过滤。

Producer在消息发送时如果设置了消息的标志属性Tag，便会存储在消息属性中，将其从`CommitLog`文件转发到消息消费队列中，consume queue会用8个字节存储Tag哈希码。（不直接存储字符串，是因为将`ConumeQueue`设计为定长结构，以加快消息消费的加载性能。）

1. Broker端拉取消息时，遍历`ConsumeQueue`，只对比Tag哈希码，如果匹配则返回，否则忽略该消息。
2. Consumer在收到消息后，同样需要先对消息进行过滤，**只是此时比较的是消息标志的值而不是哈希码。**(哈希冲突)

##### 7.1 过滤流程

1. consumer启动订阅时，构造订阅信息，包括订阅Topic与消息过滤表达式。

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#subscribe(java.lang.String, java.lang.String)`

2. 拉取消息时，获取订阅时构造的订阅信息，并向Broker发起pull Message请求。

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage`

```java
public void pullMessage(final PullRequest pullRequest) {
    // 获取订阅信息subscriptionData
    final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
    
    String subExpression = null;
    boolean classFilter = false;
    SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
    if (sd != null) {
        // 是否是class过滤
        if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
            // subExpression为Tag表达式
            subExpression = sd.getSubString();
        }
        // clase过滤模式
        classFilter = sd.isClassFilterMode();
    }
		// 系统标志
    int sysFlag = PullSysFlag.buildSysFlag(
        commitOffsetEnable, // commitOffset
        true, // suspend
        subExpression != null, // subscription
        classFilter // class filter
    );
    // 向Broker发送拉取消息请求
    try {
        this.pullAPIWrapper.pullKernelImpl(
            pullRequest.getMessageQueue(),
            subExpression,
            subscriptionData.getExpressionType(),
            subscriptionData.getSubVersion(),
            pullRequest.getNextOffset(),
            this.defaultMQPushConsumer.getPullBatchSize(),
            sysFlag,
            commitOffsetValue,
            BROKER_SUSPEND_MAX_TIME_MILLIS,
            CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            CommunicationMode.ASYNC,
            pullCallback
        );
    } catch (Exception e) {
        log.error("pullKernelImpl exception", e);
        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
    }
}
```

3. Broker处理拉取消息请求

Broker封装过滤对象：`org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)`

```java
private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
  // 构造过滤器
  MessageFilter messageFilter;
  if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
    // 支持对重试主题的过滤
    messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
  } else {
    // 不支持对重试主题的过滤
    messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
  }
  // 由过滤器来获取消息
  final GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                                                       requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);
}
```

获取消息并执行过滤：`org.apache.rocketmq.store.DefaultMessageStore#getMessage`

```java
public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter) {
  // 根据偏移量拉取消息后，首先根据ConsumeQueue条目进行消息过滤，如果不匹配则直接跳过该条消息，继续拉取下一条消息
  if (messageFilter != null
       && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
     if (getResult.getBufferTotalSize() == 0) {
       status = GetMessageStatus.NO_MATCHED_MESSAGE;
     }

     continue;
   }
  // 上面ConsumeQueue条目过滤符合条件
  // 下面需要从CommitLog文件中加载整个消息体，然后根据属性进行过滤。当然如果过滤方式是TAG模式，该方法默认返回true
  SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
  if (null == selectResult) {
    if (getResult.getBufferTotalSize() == 0) {
      status = GetMessageStatus.MESSAGE_WAS_REMOVING;
    }
    nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
    continue;
  }

  if (messageFilter != null
      && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
    if (getResult.getBufferTotalSize() == 0) {
      status = GetMessageStatus.NO_MATCHED_MESSAGE;
    }
    // release...
    selectResult.release();
    continue;
  }
```

4. 拉取消息客户端在回调方法（`PullCallback`）里面处理拉取的消息

`org.apache.rocketmq.client.impl.consumer.PullAPIWrapper#processPullResult`

```java
public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
  List<MessageExt> msgListFilterAgain = msgList;
  if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
    msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
    for (MessageExt msg : msgList) {
      if (msg.getTags() != null) {
        // 这里对比Tag真实值不是hashcode
        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
          msgListFilterAgain.add(msg);
        }
      }
    }
}
```

过滤对象关系图

<img src="https://s2.loli.net/2022/05/21/skBEtroDPHqU9Nw.png" alt="image-20220521170917368" style="zoom:33%;" />

##### 7.2 过滤机制

`ExpressionMessageFilter#isMatchedByConsumeQueue`

```java
public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
  // 订阅信息为空
  if (null == subscriptionData) {
    return true;
  }
	// 类过滤模式
  if (subscriptionData.isClassFilterMode()) {
    return true;
  }

  // by tags code.
  if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
		// 息在发送时没有设置tag
    if (tagsCode == null) {
      return true;
    }
		// Tag设置为*
    if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
      return true;
    }
		// 订阅消息的TAG hashcodes集合是否包含消息的tagsCode
    return subscriptionData.getCodeSet().contains(tagsCode.intValue());
  }
  return true;
}
```

`ExpressionMessageFilter#isMatchedByCommitLog`

```java
public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
  // 订阅信息为空
  if (subscriptionData == null) {
    return true;
  }
  // 类过滤模式
  if (subscriptionData.isClassFilterMode()) {
    return true;
  }
	// Tag过滤模式
  if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
    return true;
  }
}
```

关于`ExpressionForRetryMessageFilter`：

`ExpressionForRetryMessageFilter`继承了`ExpressionMessageFilter`，并仅仅重写了`isMatchedByCommitLog`方法。里面使用`subscriptionData.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX`)来判断是否是`isRetryTopic`；

对于`retryTopic`会使用`tempProperties.get(MessageConst.PROPERTY_RETRY_TOPIC)`来获取`realTopic`，从而根据`consumerFilterManager.get(realTopic, group)`获取`realFilterData`；最后通过`realFilterData.getCompiledExpression().evaluate(context)`来获取结果。

#### 8. 顺序消息

##### 8.1 负载均衡

`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#updateProcessQueueTableInRebalance`

经过消息队列重新负载（分配）后，分配到新的消息队列时，首先需要尝试向Broker发起锁定该消息队列的请求，如果返回加锁成功，则创建该消息队列的拉取任务，否则跳过，等待其他消费者释放
该消息队列的锁，然后在下一次队列重新负载时再尝试加锁。

顺序消息消费与并发消息消费的一个关键区别是，**顺序消息在创建消息队列拉取任务时，需要在Broker服务器锁定该消息队列**。

```java
// 检查负载均衡后的队列与Broker同步的队列
for (MessageQueue mq : mqSet) {
  // 在负载均衡队列中没有，而在Broker中有的队列
  if (!this.processQueueTable.containsKey(mq)) {
    // isOrder：顺序消费，this.lock：向Broker发送锁定队列的请求
    if (isOrder && !this.lock(mq)) {
      // 加锁失败后放弃，等待下次负载均衡加锁
      log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
      continue;
    }
		// 加锁成功后，进行队列操作和创建新的拉取消息任务pullRequest
    this.removeDirtyOffset(mq);
  } 
}
this.dispatchPullRequest(pullRequestList);
```

##### 8.2 消息拉取

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage`

如果消息处理队列未被锁定，则延迟3 s后再将`PullRequest`对象放入拉取任务中，如果该处理队列是第一次拉取任务，则首先计算拉取偏移量，然后向消息服务端拉取消息。

```java
if (processQueue.isLocked()) {
  if (!pullRequest.isPreviouslyLocked()) {
    long offset = -1L;
    try {
      offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
    } catch (Exception e) {
      this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
      return;
    }
		// 设置要拉取的偏移量
    pullRequest.setPreviouslyLocked(true);
    pullRequest.setNextOffset(offset);
  }
} else {
  // 延迟3s再执行
  this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
  return;
}
```

##### 8.3 消息消费

消息消费实现类：`org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService`

1. start()

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService#start`

20s的加锁频率与消息进行一次负载均衡的频率相同。我们知道集群模式下顺序消息消费再队列列锁定的情况下，才会
创建拉取任务，即`ProcessQueue.locked == true`。

在未锁定消息队列时无法执行消息拉取任务，`ConsumeMessageOrderlyService`以20s的频率对分配给自己的消息队列进行自动加锁操作，从而让加锁成功的队列进行消息拉取，并提交消费。

```java
public void start() {
  if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          // 集群模式下每20描执行一次，本地消息队列锁定
          ConsumeMessageOrderlyService.this.lockMQPeriodically();
        } catch (Throwable e) {
          log.error("scheduleAtFixedRate lockMQPeriodically exception", e);
        }
      }
      // ProcessQueue.REBALANCE_LOCK_INTERVAL = 20000
    }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
  }
}
```

2. 加锁

`org.apache.rocketmq.client.impl.consumer.RebalanceImpl#lockAll`

```java
public void lockAll() {
  // 根据BrokerName获取相应的队列
  HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

  Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
  while (it.hasNext()) {
    Entry<String, Set<MessageQueue>> entry = it.next();
    final String brokerName = entry.getKey();
    final Set<MessageQueue> mqs = entry.getValue();

    if (mqs.isEmpty())
      continue;
		// 获取消费者订阅Broker主节点地址
    FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
    if (findBrokerResult != null) {
      LockBatchRequestBody requestBody = new LockBatchRequestBody();
      requestBody.setConsumerGroup(this.consumerGroup);
      requestBody.setClientId(this.mQClientFactory.getClientId());
      requestBody.setMqSet(mqs);

      try {
        // 向Mater主节点发起锁定消息队列请求, 返回加锁队列lockOKMQSet
        Set<MessageQueue> lockOKMQSet =
          this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
			  // 根据加锁成功的队列lockOKMQSet, 对与本地对应的队列processQueueTable中进行设置锁定状态，同时更新加锁时间
        for (MessageQueue mq : lockOKMQSet) {
          ProcessQueue processQueue = this.processQueueTable.get(mq);
          if (processQueue != null) {
            if (!processQueue.isLocked()) {
              log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
            }

            processQueue.setLocked(true);
            processQueue.setLastLockTimestamp(System.currentTimeMillis());
          }
        }
        // 本地队列mqs(即发送锁定请求前的队列)，对于未加锁成功的队列设置锁的状态false，暂停该消息消费队列的消息拉取与消息消费
        for (MessageQueue mq : mqs) {
          if (!lockOKMQSet.contains(mq)) {
            ProcessQueue processQueue = this.processQueueTable.get(mq);
            if (processQueue != null) {
              processQueue.setLocked(false);
              log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
            }
          }
        }
      } catch (Exception e) {
        log.error("lockBatchMQ exception, " + mqs, e);
      }
    }
  }
}
```

3. 提交消费

`org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService.ConsumeRequest#run`

```java
@Override
public void run() {
  if (this.processQueue.isDropped()) {
    return;
  }
  // 获取队列锁, 并加锁
  final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
  synchronized (objLock) {
    // 广播模式或者执行队列已加锁(20s加锁一次)
    if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
      final long beginTime = System.currentTimeMillis();
      for (boolean continueConsume = true; continueConsume; ) {
        if (this.processQueue.isDropped()) {
          break;
        }
        // 一些前置条件校验，不满足则延迟执行
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
            && !this.processQueue.isLocked()) {
          ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
          break;
        }
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
            && this.processQueue.isLockExpired()) {
          ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
          break;
        }
        // 时间过长则放弃本次消费
        long interval = System.currentTimeMillis() - beginTime;
        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
          ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
          break;
        }

        final int consumeBatchSize = ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        // 从队列中获取消息，区别与并发消费（并发消费是消费刚刚拉取的那一批消息）
        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
        if (!msgs.isEmpty()) {
          final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
          ConsumeOrderlyStatus status = null;
          ConsumeMessageContext consumeMessageContext = null;
          // 钩子函数
          if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
            consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
            consumeMessageContext.setMq(messageQueue);
            consumeMessageContext.setMsgList(msgs);
            consumeMessageContext.setSuccess(false);
            // init the consume context type
            consumeMessageContext.setProps(new HashMap<String, String>());
            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
          }

          long beginTimestamp = System.currentTimeMillis();
          ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
          boolean hasException = false;
          try {
            // 申请消息消费锁
            this.processQueue.getConsumeLock().lock();
            if (this.processQueue.isDropped()) {
              break;
            }
            // 提交消费
            status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
          } catch (Throwable e) {
            hasException = true;
          } finally {
            // 释放锁
            this.processQueue.getConsumeLock().unlock();
          }

          if (null == status
              || ConsumeOrderlyStatus.ROLLBACK == status
              || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
            log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                     ConsumeMessageOrderlyService.this.consumerGroup,
                     msgs,
                     messageQueue);
          }

          long consumeRT = System.currentTimeMillis() - beginTimestamp;
          if (null == status) {
            if (hasException) {
              returnType = ConsumeReturnType.EXCEPTION;
            } else {
              returnType = ConsumeReturnType.RETURNNULL;
            }
          } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
            returnType = ConsumeReturnType.TIME_OUT;
          } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
            returnType = ConsumeReturnType.FAILED;
          } else if (ConsumeOrderlyStatus.SUCCESS == status) {
            returnType = ConsumeReturnType.SUCCESS;
          }

          if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
            consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
          }

          if (null == status) {
            status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
          }
          // 钩子函数
          if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
            consumeMessageContext.setStatus(status.toString());
            consumeMessageContext
              .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
          }

          ConsumeMessageOrderlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
          // 处理消费结果
          continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
        } else {
          continueConsume = false;
        }
      }
    } else {
      if (this.processQueue.isDropped()) {
        return;
      }

      ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
    }
  }
}
```

## ACL

1. Broker检验入口

`org.apache.rocketmq.broker.BrokerController#initialAcl`

```java
 private void initialAcl() {
     // broker.conf配置了：aclEnable=true
     if (!this.brokerConfig.isAclEnable()) {
         log.info("The broker dose not enable acl");
         return;
     }
	 // 加载校验类：AccessValidator
     List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
     if (accessValidators == null || accessValidators.isEmpty()) {
         log.info("The broker dose not load the AccessValidator");
         return;
     }

     for (AccessValidator accessValidator: accessValidators) {
         final AccessValidator validator = accessValidator;
         accessValidatorMap.put(validator.getClass(),validator);
         // 注册钩子函数
         this.registerServerRPCHook(new RPCHook() {

             @Override
             public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                 // Do not catch the exception
                 validator.validate(validator.parse(request, remoteAddr));
             }

             @Override
             public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
             }
         });
     }
 }
```









# 思考点

### 生产环境下 RocketMQ 为什么不能开启自动创建主题？

参考：https://cloud.tencent.com/developer/article/1449855。

主要是：多broker-master环境下，消息发送到一个broker-a，broker-a创建了Topic。此时broker-a没有同步新创建的Topic到NameServer（30s发一次心跳），所以broker-b上并没有新创建Topic的信息。当消息再次发送时，从NameServer拉取到的路由信息，或导致短时间内消息全部发送到有新创建的Topic的broker-a上，直到broker-b也创建新Topic，负载均衡机制才生效。

### 关于TBW102的前生今世

> 思考

	1. TBW102有什么用？
	1. autoCreateTopicEnable=false，NameServer中还是有TBW102的路由信息？

> 作用

Broker启动时，会构造TopicConfigManager。如果broker.conf设置了`autoCreateTopicEnable=true`，将会执行下述的代码，创建TBW102的路由信息，即：TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC。

TBW102路由信息创建完毕后，Broker会将路由信息注册到NameServer中，所以Proudcer在消息发送是都会拉取到Topic是TBW102的路由信息。

```java
if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
    String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
    TopicConfig topicConfig = new TopicConfig(topic);
    TopicValidator.addSystemTopic(topic);
    topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                                 .getDefaultTopicQueueNums());
    topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                                  .getDefaultTopicQueueNums());
    int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
    topicConfig.setPerm(perm);
    this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
}
```

> 两种场景（均基于Producer尝试发送一个不存在的Topic -> NotExistTopic，并且没有在Broker上手动创建Topic）：

同步路由信息步骤：`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#tryToFindTopicPublishInfo`

1. 指定Topic查找
2. 默认Topic查找（TBW102）

#### autoCreateTopicEnable=true

Broker启动，创建TBW102信息同步到NameServer。发送消息是Producer拉取到TBW102的路由信息TopicPublishInfo，并利用TBW102的路由信息来构造一个要发送Topic -> NotExistTopic的路由信息NewTopicPublishInfo。

`org.apache.rocketmq.client.impl.factory.MQClientInstance#topicRouteData2TopicPublishInfo`

Producer携带NewTopicPublishInfo发送消息到Broker，Broker发现路由NotExistTopic不存在，而且autoCreateTopicEnable=true，因此Broker利用NewTopicPublishInfo的信息创建一个新的Topic -> NotExistTopic，并将消息转存到NotExistTopic中。

`org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#msgCheck`

`org.apache.rocketmq.broker.topic.TopicConfigManager#createTopicInSendMessageMethod`



![](https://s7.51cto.com/images/blog/202106/07/0398e12d9367bdf2cf44c544a54e5a11.jpeg)

#### autoCreateTopicEnable=false

流程上与autoCreateTopicEnable=true基本一致，唯一区别是消息发送到Broker时，Broker不会创建Topic，而是返回错误信息：`topic[" + requestHeader.getTopic() + "] not exist, apply first please!`

#### 为什么TWB102一直存在

这里有个疑问：为什么autoCreateTopicEnable=false还能在NameServer中拉取到TBW102的路由信息，是不是遗留的Bug呀？

找了很久才找到的解析：https://github.com/apache/rocketmq/issues/3179

==原因解析：==

1. 构造生产者实例，调用start()方法

```java
 DefaultMQProducer producer = new DefaultMQProducer("source-producer-quick-start");
 producer.start();
```

2. start()方法中会调用到DefaultMQProducerImpl.start()

​	`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start()`

3. 构造TWB102的路由信息，并启动MQClientInstance实例

`org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start(boolean)`

```java
public void start(final boolean startFactory) throws MQClientException {
    switch (this.serviceState) {
        case CREATE_JUST:
            // 保存TBW102的路由信息到topicPublishInfoTable中
            this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

            // 启动MQClientInstance实例，同步TBW102路由信息到NameServer的关键方法
            if (startFactory) {
                mQClientFactory.start();
            }        
        case RUNNING:
        case START_FAILED:
        case SHUTDOWN_ALREADY:
            // 省略 ... 
        default:
            break;
    }
}
```

4. 定时任务同步TBW102路由信息到NameServer中

MQClientInstance启动的定时任务：`org.apache.rocketmq.client.impl.factory.MQClientInstance#start`

```java
public void start() throws MQClientException {
    synchronized (this) {
        switch (this.serviceState) {
            case CREATE_JUST:
                // Start various schedule tasks
                // 相关的定时任务
                this.startScheduledTask();
                break;
            case RUNNING:
                break;
            case SHUTDOWN_ALREADY:
                break;
            case START_FAILED:
                throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
            default:
                break;
        }
    }
}
```


```java
private void startScheduledTask() {
    // 同步路由信息的定时任务
    this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

        @Override
        public void run() {
            try {
                MQClientInstance.this.updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
            }
        }
    }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);
}
```

5. 同步路由信息到NameServer方法

`org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer()`


```java
public void updateTopicRouteInfoFromNameServer() {
    Set<String> topicList = new HashSet<String>();

    // Consumer
    {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                Set<SubscriptionData> subList = impl.subscriptions();
                if (subList != null) {
                    for (SubscriptionData subData : subList) {
                        topicList.add(subData.getTopic());
                    }
                }
            }
        }
    }

    // Producer
    {
        Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQProducerInner> entry = it.next();
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                Set<String> lst = impl.getPublishTopicList();
                topicList.addAll(lst);
            }
        }
    }

    for (String topic : topicList) {
        // broker中的每个Topic都同步到NameServer中，当然包括刚刚构造的TBW102啦
        this.updateTopicRouteInfoFromNameServer(topic);
    }
}
```

### RocketMQ性能改良

1.  `commitlog`，`consumequeue`和`index`文件长度固定以便使用内存映射机制进行文件的读写操作。
2. 文件以文件的起始**偏移量**来命令文件，这样根据偏移量能快速定位到真实的物理文件。
3. **基于内存映射文件机制**提供了**同步刷盘和异步刷盘两种机制**，异步刷盘是指在消息存储时先追加到内存映射文件，然后启动专门的刷盘线程定时将内存中的文件数据刷写到磁盘。
4. `commitlog`单一文件存储所有主题消息，并且文件顺序写，提高吞吐量，方便文件读取。
5. 构建消息消费队列文件`consumequeue`，实现了Hash索引，可以为消息设置索引键，根据所以能够快速从`CommitLog`文件中检索消息。

> `RocketMQ`不会永久存储消息文件、消息消费队列文件，而是启动文件过期机制并在磁盘空间不足或者默认凌晨4点删除过期文件，文件保存72小时并且在删除文件时并不会判断该消息文件上的消息是否被消费。

### 宕机后的数据恢复

`RocketMQ`是将消息全量存储在`CommitLog`文件中，并异步生成转发任务更新`ConsumeQueue`文件、Index文件。如果消息成功存储到`CommitLog`文件中，转发任务未成功执行，此时消息服务器Broker由于某个原因宕机，就会导致文件、`ConsumeQueue`文件、`Index`文件中的数据不一致。如果不加以人工修复，会有一部分消息即便在`CommitLog`文件中存在，由于并没有转发到`ConsumeQueue`文件，也永远不会被消费者消费。

存储启动时所谓的文件恢复主要完成`flushedPosition`、`committedWhere`指针的设置、将消息消费队列最大偏移量加载到内存，并删除`flushedPosition`之后所有的文件。如果Broker异常停止，在文件恢复过程中，会将最后一个有效文件中的所有消息重新转发到`ConsumeQueue`和`Index`文件中，确保不丢失消息，但同时会带来消息重复的问题。纵观`RocktMQ`的整体设计思想，`RocketMQ`保证消息不丢失但不保证消息不会重复消费，故消息消费业务方需要实现消息消费的幂等设计。

`org.apache.rocketmq.store.DefaultMessageStore#load`

> 1.  判断上一次退出是否正常。

其实现机制是Broker在启动时创建${ROCKET_HOME}/store/abort文件，在退出时通过注册`JVM`钩子函数删除abort文件。如果下一次启动时存在abort文件。说明Broker是异常退出的，`CommitLog`与`ConsumeQueue`数据有可能不一致，需要进行修复。

> 2. 加载commitlog

`org.apache.rocketmq.store.CommitLog#load`

> 3. 加载消费队列Consume Queue

`org.apache.rocketmq.store.DefaultMessageStore#loadConsumeQueue`

> 4. 加载存储checkpoint文件

记录`CommitLog`文件、`ConsumeQueue`文件、`Index`文件的刷盘点。

> 5. 加载Index文件

如果上次异常退出，而且Index文件刷盘时间小于该文件最大的消息时间戳，则该文件将立即销毁。

> 6. recover

根据Broker是否为正常停止，执行不同的恢复策略，下文将分别介绍异常停止、正常停止的文件恢复机制。

`org.apache.rocketmq.store.DefaultMessageStore#recover`

> 7. 加载延迟队列

### 流量控制8大场景

参看文档：https://heapdump.cn/article/3712290，https://cloud.tencent.com/developer/article/1456404

- Broker(Producer)

`org.apache.rocketmq.broker.latency.BrokerFastFailure#start`

默认情况下，broker开启流控开关：`brokerFastFailureEnable = true`，broker每隔10毫秒会做一次流控处理。处理方式：从队列中获取一个请求，设置响应码`RemotingSysResponseCode.SYSTEM_BUSY`，返回给Producer。注意Producer不会对此响应码做消息重试。

```java
// page busy 流控处理
if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
     // 此处为poll， 
     final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
     if (null == runnable) {
         break;
     }

     final RequestTask rt = castRunnable(runnable);
     rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", System.currentTimeMillis() - rt.getCreateTimestamp(), this.brokerController.getSendThreadPoolQueue().size()));
 } else {
     break;
 }

// 
if (!blockingQueue.isEmpty()) {
    final Runnable runnable = blockingQueue.peek();
    if (null == runnable) {
        break;
    }
    final RequestTask rt = castRunnable(runnable);
    if (rt == null || rt.isStopRun()) {
        break;
    }

    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
    if (behind >= maxWaitTimeMillsInQueue) {
        if (blockingQueue.remove(runnable)) {
            rt.setStopRun(true);
            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
        }
    } else {
        break;
    }
} else {
    break;
}
```





判断方式如下：

> `BrokerFastFailure`

1. Page Cache 繁忙
   - 获取 `CommitLog` 写入锁，如果持有锁的时间大于 `osPageCacheBusyTimeOutMills`（默认 `1s`）
2. 清理过期请求

清理过期请求时，如果请求线程的创建时间到当前系统时间间隔大于 `waitTimeMillsInSendQueue`（默认 200 ms，可以配置）就会清理这个请求。

> `NettyRemotingAbstract`

1. system busy

`NettyRemotingAbstract#processRequestCommand`

```java
// 拒绝请求
if (pair.getObject1().rejectRequest()) {
    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                                                                           "[REJECTREQUEST]system busy, start flow control for a while");
    response.setOpaque(opaque);
    ctx.writeAndFlush(response);
    return;
}
// 两种情况：1. page cache繁忙 2. 开启TransientStorePoolDeficient，堆外的buffer=0没有空闲
public boolean rejectRequest() {
    return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
        this.brokerController.getMessageStore().isTransientStorePoolDeficient();
}
```

2. 线程池拒绝

Broker 收到请求后，会把处理逻辑封装成到 Runnable 中，由线程池来提交执行，如果线程池满了就会拒绝请求（这里线程池中队列的大小默认是 10000，可以通过参数 `sendThreadPoolQueueCapacity` 进行配置），线程池拒绝后会抛出异常 `RejectedExecutionException`，程序捕获到异常后，会判断是不是单向请求（`OnewayRPC`），如果不是，就会给 Producer 返回一个系统繁忙的状态码（code=2，remark="[OVERLOAD]system busy, start flow control for a while"）

- Consumer

`org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage`

消费者在拉取消息时会检查流控要求，若超过预设的阈值，将会触发消息流控，放弃本次消息拉取并且该队列的下一次拉取任务将在50 ms后才加入拉取任务队列。

1. 缓存消息数量超过阈值和缓存消息大小超过阈值(`cachedMessageCount`, `cachedMessageSizeInMiB`)

```java
long cachedMessageCount = processQueue.getMsgCount().get();
long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
    this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
    if ((queueFlowControlTimes++ % 1000) == 0) {
        log.warn(
            "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
            this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
    }
    return;
}

if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
    this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
    if ((queueFlowControlTimes++ % 1000) == 0) {
        log.warn(
            "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
            this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
    }
    return;
}
```

2. 并发消费：`ProcessQueue`中队列最大偏移量与最小偏离量的间距不能超过`consumeConcurrently MaxSpan`，否则触发流控。这里主要的考量是担心因为一条消息堵塞，使消息进度无法向前推进，可能会造成大量消息重复消费。

```java
if (!this.consumeOrderly) {
    if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
        this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
        if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
            log.warn(
                "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                pullRequest, queueMaxSpanFlowControlTimes);
        }
        return;
    }
}
```

3. 顺序消费

对于顺序消费的情况，`ProcessQueue` 加锁失败，也会延迟拉取，这个延迟时间默认是 3 s，可以配置。

```java
 if (processQueue.isLocked()) {
     if (!pullRequest.isLockedFirst()) {
         final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
         boolean brokerBusy = offset < pullRequest.getNextOffset();
         log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                  pullRequest, offset, brokerBusy);
         if (brokerBusy) {
             log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                      pullRequest, offset);
         }

         pullRequest.setLockedFirst(true);
         pullRequest.setNextOffset(offset);
     }
 } else {
     this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
     log.info("pull message later because not locked in broker, {}", pullRequest);
     return;
 }
```

- 总结	

`RocketMQ` 发生流量控制的 8 个场景，其中 Broker 4 个场景，Consumer 4 个场景。Broker 的流量控制，本质是对 Producer 的流量控制，最好的解决方法就是给 Broker 扩容，增加 Broker 写入能力。而对于 Consumer 端的流量控制，需要解决 Consumer 端消费慢的问题，比如有第三方接口响应慢或者有慢 SQL。

在使用的时候，根据打印的日志可以分析具体是哪种情况的流量控制，并采用相应的措施。

### 重复消费产生地方

1. consumer消费消息后就会根据偏移量来移除本地的消息，提交消费进度。多线程场景下，每次提交只会提交本地消息的最小偏移量，因此消息拉取线程会根据偏移量拉取消息到本地进行消费。这是就会拉取到已经消费的消息，重复消费，因为此时消息进度是最少偏移量。

```java
// 本地消息保存在ProcessQueue.msgTreeMap
TreeMap<Long /*偏移量*/, MessageExt /*消息*/> msgTreeMap = new TreeMap<Long, MessageExt>();
```

### 疑问点，待解决

1. 延迟消息，重试消息都是创建一个msgId，放在一个统一Topic里面，不影响正常消息的消费进度吗？

   是的，可参看消息确认过程。



