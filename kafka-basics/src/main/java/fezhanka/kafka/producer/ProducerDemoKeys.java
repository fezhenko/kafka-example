package fezhanka.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        // producer props
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://exotic-pony-12538-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"ZXhvdGljLXBvbnktMTI1Mzgk3fhqlTT3iQobb8-SVP_zWySNXgHWVt03FoO_VIo\" password=\"YmYxMmJlNTAtN2IwZC00NWZjLTgyZjgtOWE0MTA1MTIzNDlh\";"
        );
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("batch.size", "400");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String topic = "fzhnk.kafka.example";

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String key = "id_" + j;
                String value = "example-message" + j;

                kafkaProducer.send(
                        new ProducerRecord<>(topic, key, value),
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                // execute every time a record successfully sent
                                if (e == null) {
                                    log.info("Topic: {}", recordMetadata.topic());
                                    log.info("key={} value={}", key, value);
                                    log.info("Partition: {}", recordMetadata.partition());
                                } else {
                                    log.error("Error occurred while sending message", e);
                                }
                            }
                        }
                );
            }
        }

        kafkaProducer.close(); // close include flush() to send data and block until done -- synchronous
    }
}
