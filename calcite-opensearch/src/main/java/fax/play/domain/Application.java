package fax.play.domain;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.Closer;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import fax.play.opensearch.OpenSearchClient;
import fax.play.opensearch.OpenSearchServer;

public class Application implements AutoCloseable {

   public static final int NUM_TABLES = 5;
   public static final int NUM_ROWS = 10_000;

   private static final Logger LOG = LoggerFactory.getLogger(Application.class);

   private final Closer closer = new Closer();

   private final OpenSearchClient openSearchClient;

   public Application() {
      OpenSearchServer openSearchServer = new OpenSearchServer();
      Map<String, String> serverConfig = openSearchServer.start();
      closer.add(openSearchServer);
      openSearchClient = new OpenSearchClient(
            serverConfig.get("username"), serverConfig.get("password"), serverConfig.get("host"));
      closer.add(openSearchClient);
   }

   public void start() throws IOException {
      indexing();
   }

   public Connection createConnection() throws SQLException {
      Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
      SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
      root.add("elastic", new ElasticsearchSchema(openSearchClient.getRestClient(), new ObjectMapper(), null));

      defineViews(root);
      closer.add(connection);

      return connection;
   }

   @Override
   public void close() {
      closer.close();
   }

   private void indexing() throws IOException {
      Response response = openSearchClient.createIndex("table_1", Json.object(
            "id", Json.object("type", "integer")
      ));
      LOG.info(EntityUtils.toString(response.getEntity()));

      for (int i = 2; i <= NUM_TABLES; i++) {
         response = openSearchClient.createIndex("table_" + i, Json.object(
               "id", Json.object("type", "integer"),
               "table_" + (i - 1) + "_id", Json.object("type", "integer")));
         LOG.info(EntityUtils.toString(response.getEntity()));
      }

      List<Integer> range = IntStream.rangeClosed(1, NUM_ROWS).boxed().toList();
      Map<String, Json> documents =
            range.stream().collect(Collectors.toMap((key) -> key + "", (key) -> Json.object("id", key)));
      openSearchClient.bulkIndexing("table_1", documents);

      Random rand = new Random();

      for (int i = 2; i <= NUM_TABLES; i++) {
         documents = new HashMap<>(NUM_ROWS);
         for (int j = 0; j < NUM_ROWS; j++) {
            int id = j + 1;
            String docId = id + "";
            Json json = Json.object("id", id, "table_" + (i - 1) + "_id", rand.nextInt(NUM_ROWS+1));
            documents.put(docId, json);
         }
         openSearchClient.bulkIndexing("table_" + i, documents);
      }
   }

   private void defineViews(SchemaPlus root) {
      String viewSql = "select"
            + " cast(_MAP['id'] AS integer) AS \"id\""
            + " from  \"elastic\".\"table_1\"";

      root.add("table_1",
            ViewTable.viewMacro(root, viewSql,
                  Collections.singletonList("elastic"),
                  Arrays.asList("elastic", "view"), false));

      for (int i = 2; i <= NUM_TABLES; i++) {
         String tableName = "table_" + i;
         String foreignKeyName = "table_" + (i - 1) + "_id";

         viewSql = "select cast(_MAP['" + foreignKeyName + "'] AS integer) AS \"" + foreignKeyName + "\" ,"
               + " cast(_MAP['id'] AS integer) AS \"id\""
               + " from \"elastic\".\"" + tableName + "\"";

         root.add(tableName,
               ViewTable.viewMacro(root, viewSql,
                     Collections.singletonList("elastic"),
                     Arrays.asList("elastic", "view"), false));
      }
   }
}
