package fax.play.extractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Extractor {

   public static List<?> columnExtractor(ResultSet resultSet) throws SQLException {
      if (resultSet.getMetaData().getColumnCount() == 1) {
         return singleColumnExtraction(resultSet);
      }
      return multipleColumnExtraction(resultSet);
   }

   private static List<Object> singleColumnExtraction(ResultSet resultSet) throws SQLException {
      ArrayList<Object> result = new ArrayList<>();
      assert resultSet.getMetaData().getColumnCount() == 1;
      while (resultSet.next()) {
         result.add(resultSet.getObject(1));
      }
      return result;
   }

   private static List<List<Object>> multipleColumnExtraction(ResultSet resultSet) throws SQLException {
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

}
