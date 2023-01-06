package fax.play.opensearch;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;

public class OpenSearchClient implements AutoCloseable {

   private final RestClient restClient;

   public OpenSearchClient(String username, String password, String host) {
      restClient = restClient(username, password, host);
   }

   public RestClient getRestClient() {
      return restClient;
   }

   @Override
   public void close() throws IOException {
      restClient.close();
   }

   public Response createIndex(String index, Json properties) throws IOException {
      Request request = new Request("PUT", "/" + index);

      Json mappings = Json.object("mappings",
            Json.object("properties", properties));
      request.setJsonEntity(mappings.toString());

      return restClient.performRequest(request);
   }

   public Response bulkIndexing(String index, Map<String, Json> documents) throws IOException {
      Request request = new Request("POST", "/_bulk");

      request.addParameter("refresh", "true");

      StringBuilder body = new StringBuilder();
      for (Map.Entry<String, Json> entry : documents.entrySet()) {
         body.append(Json.object("index", Json.object("_index", index, "_id", entry.getKey())));
         body.append("\n"); // using \n and not the system line separator, since the value will be used by the server VM
         body.append(entry.getValue()); // make the single entity single line
         body.append("\n"); // using \n and not the system line separator, since the value will be used by the server VM
      }
      request.setJsonEntity(body.toString());

      return restClient.performRequest(request);
   }

   private static RestClient restClient(String username, String password, String host) {
      return RestClient.builder(HttpHost.create(host))
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                  .setConnectTimeout(60 * 1000)  // default 1000
                  .setSocketTimeout(3 * 60 * 1000))  // default 30000
            .setHttpClientConfigCallback(
                  httpClientBuilder -> {
                     final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                     credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

                     httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                     return httpClientBuilder;
                  }
            )
            .build();
   }
}
