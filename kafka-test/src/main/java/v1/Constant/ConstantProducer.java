package v1.Constant;

/**
 * Created by 張燿峰
 * Kafka生产者参数配置
 * @author 孤
 * @date 2019/4/25
 * @Varsion 1.0
 */
public class ConstantProducer {

    /**
     * 用于建立与kafka集群连接的host/port组。
     * 数据将会在所有servers上均衡加载，不管哪些server是指定用于bootstrapping。
     * 这个列表仅仅影响初始化的hosts（用于发现全部的servers）。这个列表格式：host1:port1,host2:port2,...
     * 因为这些server仅仅是用于初始化的连接，以发现集群所有成员关系（可能会动态的变化），
     * 这个列表不需要包含所有的servers（你可能想要不止一个server，尽管这样，可能某个server宕机了）。
     * 如果没有server在这个列表出现，则发送数据会一直失败，直到列表可用。
     */
    public static String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * producer需要server接收到数据之后发出的确认接收的信号，此项配置就是指procuder需要多少个这样的确认信号。
     * 此配置实际上代表了数据备份的可用性。以下设置为常用选项：
     * acks=0： 设置为0表示producer不需要等待任何确认收到的信息。副本将立即加到socket  buffer并认为已经发送。没有任何保障可以保证此种情况下server已经成功接收数据，同时重试配置不会发生作用（因为客户端不知道是否失败）回馈的offset会总是设置为-1；
     * acks=1： 这意味着至少要等待leader已经成功将数据写入本地log，但是并没有等待所有follower是否成功写入。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
     * acks=all： 这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
     * 其他他的设置，例如acks=2也是可以的，这将需要给定的acks数量，但是这种策略一般很少用。
     */
    public static String ACKS = "acks";

    /**
     * 设置大于0的值将使客户端重新发送任何数据，一旦这些数据发送失败。
     * 注意，这些重试与客户端接收到发送错误时的重试没有什么不同。
     * 允许重试将潜在的改变数据的顺序，
     * 如果这两个消息记录都是发送到同一个partition，则第一个消息失败第二个发送成功，则第二条消息会比第一条消息出现要早。
     */
    public static String RETRIES = "retries";

    /**
     * producer将试图批处理消息记录，以减少请求次数。这将改善client与server之间的性能。这项配置控制默认的批量处理消息字节数。
     * 不会试图处理大于这个字节数的消息字节数。
     * 发送到brokers的请求将包含多个批量处理，其中会包含对每个partition的一个请求。
     * 较小的批量处理数值比较少用，并且可能降低吞吐量（0则会仅用批量处理）。
     * 较大的批量处理数值将会浪费更多内存空间，这样就需要分配特定批量处理数值的内存大小。
     */
    public static String BATCH_SIZE = "batch.size";

    /**
     * producer组将会汇总任何在请求与发送之间到达的消息记录一个单独批量的请求。
     * 通常来说，这只有在记录产生速度大于发送速度的时候才能发生。
     * 然而，在某些条件下，客户端将希望降低请求的数量，甚至降低到中等负载一下。
     * 这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理。
     * 这可以认为是TCP种Nagle的算法类似。这项设置设定了批量处理的更高的延迟边界：一旦我们获得某个partition的batch.size，他将会立即发送而不顾这项设置，然而如果我们获得消息字节数比这项设置要小的多，我们需要“linger”特定的时间以获取更多的消息。
     * 这个设置默认为0，即没有延迟。设定linger.ms=5，例如，将会减少请求数目，但是同时会增加5ms的延迟。
     */
    public static String LINGER_MS = "linger.ms";

    /**
     * producer可以用来缓存数据的内存大小。
     * 如果数据产生速度大于向broker发送的速度，producer会阻塞或者抛出异常，以“block.on.buffer.full”来表明。
     * 这项设置将和producer能够使用的总内存相关，但并不是一个硬性的限制，因为不是producer使用的所有内存都是用于缓存。
     * 一些额外的内存会用于压缩（如果引入压缩机制），同样还有一些用于维护请求。
     */
    public static String BUFFER_MEMORY = "buffer.memory";

    /**
     * 关键字的序列化类。如果没给与这项，默认情况是和消息一致
     */
    public static String KEY_SERIALIZER_CLASS = "key.serializer";

    /**
     * 关键字的系列化类
     */
    public static String VALUE_SERIALIZER_CLASS = "value.serializer";
}
