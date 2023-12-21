package org.kafka.producer;

import com.kafka.protobuf.MyRecordProto.MyRecord;
import com.kafka.protobuf.OtherRecordProto.OtherRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;

public class ProducerSchemaRegistry {
    public static void main(String[] args) {
        Producer<String, MyRecord> producer = new KafkaProducer<>(getProperties());
        String topic = "testproto";
        String key = "testkey";
        OtherRecord otherRecord = OtherRecord.newBuilder()
                .setOtherId(123).build();
        MyRecord myrecord = MyRecord.newBuilder()
                .setF1("value teste").setF2(otherRecord).build();

        ProducerRecord<String, MyRecord> record
                = new ProducerRecord<>(topic, key, myrecord);

        producer.send(record);
        producer.flush();
        producer.close();
    }

    private static Properties getProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        properties.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(AUTO_REGISTER_SCHEMAS, "true");
        return properties;
    }
}