package fzhnk.kafka.opensearch.consumer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class OpenSearchConsumer {

    private Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://exotic-pony-12538-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"ZXhvdGljLXBvbnktMTI1Mzgk3fhqlTT3iQobb8-SVP_zWySNXgHWVt03FoO_VIo\" password=\"YmYxMmJlNTAtN2IwZC00NWZjLTgyZjgtOWE0MTA1MTIzNDlh\";"
        );
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "fzhnk-test-opensearch-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        return props;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<>(getConsumerProps());
    }
}
