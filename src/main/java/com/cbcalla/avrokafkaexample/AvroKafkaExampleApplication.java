package com.cbcalla.avrokafkaexample;

import com.cbcalla.avrokafkaexample.models.avro.Rx;
import com.cbcalla.avrokafkaexample.models.avro.RxStatus;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static ch.qos.logback.core.encoder.ByteArrayUtil.hexStringToByteArray;

@SpringBootApplication
public class AvroKafkaExampleApplication implements CommandLineRunner {

    private static Logger logger = LoggerFactory.getLogger(AvroKafkaExampleApplication.class);

    private static final String TOPIC = "RX-CREATED";
    private static final String CLIENT_ID = "RX-CLIENT";
    private static final String GROUP = "RX-CONSUMER";

    public static void main(String[] args) {
        SpringApplication.run(AvroKafkaExampleApplication.class, args);
    }

    @Override
    public void run(String... args) {

        // Create Avro object
        Rx rx = Rx.newBuilder()
                .setRxId("3030")
                .setRxNbr("4040")
                .setPatientId(5000)
                .setStatus(RxStatus.TRANSFERRED)
                .build();

        // Serialize and push to Kafka
        var producer = createProducer();
        var producerRecord = new ProducerRecord<String, Rx>(TOPIC, "myKey", rx);
        producerRecord.headers().add("SomeHeader", hexStringToByteArray("e04fd020ea3a6910a2d808002b30309d"));

        producer.send(producerRecord);

        producer.flush();
        producer.close();

        // Consume from Kafka and deserialize
        var consumer = createConsumer();

        // Subscribe to topic
        consumer.subscribe(Collections.singletonList(TOPIC));

        final ConsumerRecords<String, Rx> records = consumer.poll(Duration.ofSeconds(1));

        if (records.count() == 0) {
            System.out.println("None found");
        } else records.forEach(record -> {
            Rx rxRecord = record.value();
            System.out.printf("%s %d %d %s %s\n", record.topic(), record.partition(), record.offset(), record.key(), rxRecord.toString());
        });
    }

    private static Producer<String, Rx> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaProducer<>(props);
    }

    private static Consumer<String, Rx> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);

        // The offset isn't getting saved in my dev setup for some reason
        // so I just get all the messages for testing purposes
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaConsumer<>(props);
    }

}
