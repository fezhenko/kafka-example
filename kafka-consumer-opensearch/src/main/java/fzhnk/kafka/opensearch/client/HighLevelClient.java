package fzhnk.kafka.opensearch.client;

import java.net.URI;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

public class HighLevelClient {

    public RestHighLevelClient getRestHighLevelClient() {
        URI uri = URI.create("http://localhost:9200");

        if (uri.getUserInfo() == null) {
            // client without security
            return new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())));
        }

        String[] auth = uri.getUserInfo().split(":");
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                        )
        );
    }
}
