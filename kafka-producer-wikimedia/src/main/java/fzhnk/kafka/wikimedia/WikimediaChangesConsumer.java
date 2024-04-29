package fzhnk.kafka.wikimedia;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesConsumer {

    public static final Logger log = LoggerFactory.getLogger(WikimediaChangesConsumer.class.getSimpleName());

    public static void main(String[] args) {

        String bootstrapServer = "https://exotic-pony-12538-eu2-kafka.upstash.io:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"ZXhvdGljLXBvbnktMTI1Mzgk3fhqlTT3iQobb8-SVP_zWySNXgHWVt03FoO_VIo\" password=\"YmYxMmJlNTAtN2IwZC00NWZjLTgyZjgtOWE0MTA1MTIzNDlh\";"
        );

        // consumer props
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "fzhnk-test-wikimedia");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        // create consumer
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(props);

        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            kafkaConsumer.subscribe(List.of("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received record: {}", record);
                }
            }
        } catch (WakeupException e) {
            log.info("stopping consumer thread");
        } finally {
            kafkaConsumer.close();
        }
    }
}
