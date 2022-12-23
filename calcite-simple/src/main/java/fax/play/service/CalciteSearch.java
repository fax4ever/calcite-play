package fax.play.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CalciteSearch {

   private final RestClient restClient;
   private final List<String> indexes;
   private final ObjectMapper objectMapper = new ObjectMapper();

   public CalciteSearch(RestClient restClient, List<String> indexes) {
      this.restClient = restClient;
      this.indexes = indexes;
   }

   public Connection createConnection() throws SQLException {
      Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
      SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

      for (String index : indexes) {
         root.add("elastic", new ElasticsearchSchema(restClient, objectMapper, index));
      }

      return connection;
   }

}
