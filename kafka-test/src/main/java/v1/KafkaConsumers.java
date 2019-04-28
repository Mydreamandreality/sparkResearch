package v1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import v1.Constant.ConstantConsumer;
import v1.Constant.ConstantProducer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by 張燿峰
 * kafka消费者代码案例
 *
 * @author 孤
 * @date 2019/4/25
 * @Varsion 1.0
 */
public class KafkaConsumers extends Thread{
    private static KafkaConsumer<String, String> consumer;
    private static final String TOPIC = "JavaKafka";

    public KafkaConsumers() {
        Properties properties = new Properties();
        properties.put(ConstantProducer.BOOTSTRAP_SERVERS, "192.168.253.132:9092");
        properties.put(ConstantConsumer.GROUP_ID, "test-consumer-group");
        properties.put(ConstantConsumer.ENABLE_AUTO_COMMIT, "true");
        properties.put(ConstantConsumer.AUTO_COMMIT_INTERVAL_MS, 1000);
        properties.put(ConstantConsumer.SESSION_TIMEOUT_MS, 30000);
        properties.put(ConstantConsumer.AUTO_OFFSET_RESET, "earliest");
        properties.put(ConstantConsumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConstantConsumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void getConsumers() {
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            //TODO 成功执行后 输出文档~
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> consumerRecords : records) {
                System.out.println("key:" + consumerRecords.key() + ", value: " + consumerRecords.value() + ", topic: " + consumerRecords.topic());
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumers().getConsumers();
    }
}
