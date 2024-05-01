package fzhnk.kafka.opensearch;

import com.google.gson.JsonParser;
import fzhnk.kafka.opensearch.client.HighLevelClient;
import fzhnk.kafka.opensearch.consumer.OpenSearchConsumer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    static Logger log = LoggerFactory.getLogger(Main.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        // create OpenSearch Client
        HighLevelClient highLevelClient = new HighLevelClient();
        RestHighLevelClient client = highLevelClient.getRestHighLevelClient();

        // create consumer
        OpenSearchConsumer openSearchConsumer = new OpenSearchConsumer();
        KafkaConsumer<String, String> consumer = openSearchConsumer.getConsumer();
        consumer.subscribe(List.of("wikimedia.recentchange"));

        // client will be autoclosed
        try (client; consumer) {

            boolean ifExists = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            // create OpenSearch index if it does not exist
            if (!ifExists) {
                CreateIndexRequest wikimediaIndex = new CreateIndexRequest("wikimedia");
                client.indices().create(wikimediaIndex, RequestOptions.DEFAULT);
                log.info("index has been created");
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                log.info("records count: {}", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    bulkRequest.add(new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(extractId(record.value())));
                }

                if (bulkRequest.numberOfActions() == 0) {
                    log.info("nothing to do");
                } else {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted: {} item(s)", bulkResponse.getItems().length);
                }
            }
        }
    }

    private static String extractId(String record) {
        // gson
        return JsonParser.parseString(record)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
