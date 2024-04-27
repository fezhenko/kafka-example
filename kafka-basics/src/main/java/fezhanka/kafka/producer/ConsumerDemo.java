package fezhanka.kafka.producer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        // common props
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://exotic-pony-12538-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXhvdGljLXBvbnktMTI1Mzgk3fhqlTT3iQobb8-SVP_zWySNXgHWVt03FoO_VIo\" password=\"YmYxMmJlNTAtN2IwZC00NWZjLTgyZjgtOWE0MTA1MTIzNDlh\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // consumer configs
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "fzhnk.consumer.demo");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // subscribe to a topic
        kafkaConsumer.subscribe(List.of("fzhnk.kafka.example"));

        // poll for a data
        while (true) {
            log.info("polling");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("key={}, value={}", record.key(), record.value());
                log.info("partition={}, offset={}", record.partition(), record.offset());
            }
        }


    }
}
