package org.kafka.consumer;

import com.kafka.protobuf.MyRecordProto.MyRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        String topic = "testproto";
        KafkaConsumer<String, MyRecord> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(Collections.singleton(topic));


        try {
            while (true) {
                ConsumerRecords<String, MyRecord> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, MyRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n",
                            record.offset(), record.key(), record.value());
                }
                consumer.commitAsync();
            }
        }catch (Exception e){
          System.out.println(e);
        }finally {
            consumer.close();
        }
    }

    private static Properties getProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-protobuf-consumer-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        properties.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return properties;
    }
}
