package fax.play.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CalciteSearch {

   private final RestClient restClient;
   private final ObjectMapper objectMapper = new ObjectMapper();

   public CalciteSearch(RestClient restClient) {
      this.restClient = restClient;
   }

   public Connection createConnection() throws SQLException {
      Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
      SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
      root.add("elastic", new ElasticsearchSchema(restClient, objectMapper, null));

      return connection;
   }

}
