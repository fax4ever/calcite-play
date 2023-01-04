package fax.play.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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
      return createConnectionWith(null);
   }

   public Connection createConnectionWith(Consumer<SchemaPlus> views) throws SQLException {
      Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
      SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
      root.add("elastic", new ElasticsearchSchema(restClient, objectMapper, null));

      if (views != null) {
         views.accept(root);
      }

      return connection;
   }

   public <K> List<K> executeQuery(String sql, CheckedSQLFunction<ResultSet, List<K>> extractor) throws SQLException {
      return executeQuery(null, sql, extractor);
   }

   public <K> List<K> executeQuery(Consumer<SchemaPlus> views, String sql, CheckedSQLFunction<ResultSet, List<K>> extractor) throws SQLException {
      try (Connection connection = createConnectionWith(views)) {
         try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
               return extractor.apply(resultSet);
            }
         }
      }
   }

   public static List<Object> singleColumnExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<Object> result = new ArrayList<>();
      assert resultSet.getMetaData().getColumnCount() == 1;
      while (resultSet.next()) {
         result.add(resultSet.getObject(1));
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   public static List<Map<String, Object>> singleColumnMapExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<Map<String, Object>> result = new ArrayList<>();
      assert resultSet.getMetaData().getColumnCount() == 1;
      while (resultSet.next()) {
         result.add((Map<String, Object>) resultSet.getObject(1));
      }
      return result;
   }

   public static List<List<Object>> multipleColumnExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<List<Object>> result = new ArrayList<>();
      int columnCount = resultSet.getMetaData().getColumnCount();
      assert columnCount > 1;
      while (resultSet.next()) {
         ArrayList<Object> row = new ArrayList<>(columnCount);
         for (int i=0; i<columnCount; i++) {
            row.add((resultSet.getObject(i+1)));
         }
         result.add(row);
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   public static List<List<Map<String, Object>>> multipleColumnMapExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<List<Map<String, Object>>> result = new ArrayList<>();
      int columnCount = resultSet.getMetaData().getColumnCount();
      assert columnCount > 1;
      while (resultSet.next()) {
         ArrayList<Map<String, Object>> row = new ArrayList<>(columnCount);
         for (int i=0; i<columnCount; i++) {
            row.add((Map<String, Object>) resultSet.getObject(i+1));
         }
         result.add(row);
      }
      return result;
   }
}
