package v1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import v1.Constant.ConstantProducer;

import java.util.Properties;

/**
 * Created by 張燿峰
 * kafka生产者代码案例
 *
 * @author 孤
 * @date 2019/4/25
 * @Varsion 1.0
 */
public class KafkaProducers extends Thread{

    private static KafkaProducer producer;
    private static final String TOPIC = "JavaKafka";

    public KafkaProducers() {
        Properties properties = new Properties();
        properties.put(ConstantProducer.BOOTSTRAP_SERVERS, "192.168.253.132:9092");
        properties.put(ConstantProducer.ACKS, "all");
        properties.put(ConstantProducer.RETRIES, 0);
        properties.put(ConstantProducer.BATCH_SIZE, 16385);
        properties.put(ConstantProducer.LINGER_MS, 1);
        properties.put(ConstantProducer.BUFFER_MEMORY, 33554432);
        properties.put(ConstantProducer.KEY_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConstantProducer.VALUE_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(properties);
    }

    public void sendProducer() {
        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);
            String data  = "hello message "+ key;
            producer.send(new ProducerRecord<>(TOPIC,key,data));
            System.out.println("SUCCESS~");
        }
        producer.close();
    }

    public static void main(String[] args) {
        new KafkaProducers().sendProducer();
    }
}
