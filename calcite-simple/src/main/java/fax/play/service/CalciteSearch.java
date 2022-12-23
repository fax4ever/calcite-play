package fax.play.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

   public <K> List<K> executeQuery(String sql, CheckedSQLFunction<ResultSet, List<K>> extractor) throws SQLException {
      try (Connection connection = createConnection()) {
         try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
               return extractor.apply(resultSet);
            }
         }
      }
   }

   @SuppressWarnings("unchecked")
   public static List<Map<String, Object>> singleColumnMapExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<Map<String, Object>> result = new ArrayList<>();
      assert resultSet.getMetaData().getColumnCount() == 1; // this is the way the ES adapter works
      while (resultSet.next()) {
         result.add((Map<String, Object>) resultSet.getObject(1));
      }
      return result;
   }
}
