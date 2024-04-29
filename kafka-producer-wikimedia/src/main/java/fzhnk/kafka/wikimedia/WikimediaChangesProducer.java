package fzhnk.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "https://exotic-pony-12538-eu2-kafka.upstash.io:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXhvdGljLXBvbnktMTI1Mzgk3fhqlTT3iQobb8-SVP_zWySNXgHWVt03FoO_VIo\" password=\"YmYxMmJlNTAtN2IwZC00NWZjLTgyZjgtOWE0MTA1MTIzNDlh\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // set high throughput producer configs
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(new KafkaProducer<>(props), "wikimedia.recentchange");
        EventSource eventSource = new EventSource
                .Builder(eventHandler, URI.create(url))
                .build();

        // start the producer in another thread
        eventSource.start();

        // produce for 5-min and block until then
        TimeUnit.MINUTES.sleep(5);

    }
}
