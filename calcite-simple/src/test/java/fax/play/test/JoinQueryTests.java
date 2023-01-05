package fax.play.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.Closer;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fax.play.resource.OpenSearchContainer;
import fax.play.service.CalciteSearch;
import fax.play.service.SearchService;

public class JoinQueryTests {

   /**
    * SELECT
    * count(*)
    * FROM
    * table_1 AS t1 INNER JOIN
    * table_2 AS t2 ON
    * t1.id = t2.table_1_id INNER JOIN
    * table_3 AS t3 ON
    * t2.id = t3.table_2_id INNER JOIN
    * table_4 AS t4 ON
    * t3.id = t4.table_3_id INNER JOIN
    * table_5 AS t5 ON
    * t4.id = t5.table_4_id
    * WHERE
    * t1.id <= 10;
    */

   /**
    * CREATE TABLE table_1s (
    * id serial primary key,
    * table_2_id integer references table_2s (id)
    * );
    */

   private static final int NUM_TABLES = 5;
   private static final int NUM_ROWS = 10_000;

   private static final Logger LOG = LoggerFactory.getLogger(JoinQueryTests.class);

   private static OpenSearchContainer searchContainer;
   private static Map<String, String> start;
   private static SearchService searchService;
   private static Closer closer = new Closer();

   private final CalciteSearch calciteSearch = new CalciteSearch(searchService.getRestClient());

   @BeforeAll
   public static void beforeAll() throws Exception {
      searchContainer = new OpenSearchContainer();
      start = searchContainer.start();
      closer.add(searchContainer);
      searchService = new SearchService(start.get("username"), start.get("password"), start.get("host"));
      closer.add(searchService);

      indexing();
   }

   @AfterAll
   public static void afterAll() {
      closer.close();
   }

   private static void indexing() throws Exception {
      Response response = searchService.createIndex("table_1", Json.object(
            "id", Json.object("type", "integer")
      ));
      LOG.info(EntityUtils.toString(response.getEntity()));

      for (int i = 2; i <= NUM_TABLES; i++) {
         response = searchService.createIndex("table_" + i, Json.object(
               "id", Json.object("type", "integer"),
               "table_" + (i - 1) + "_id", Json.object("type", "integer")));
         LOG.info(EntityUtils.toString(response.getEntity()));
      }

      List<Integer> range = IntStream.rangeClosed(1, NUM_ROWS).boxed().toList();
      Map<String, Json> documents =
            range.stream().collect(Collectors.toMap((key) -> key + "", (key) -> Json.object("id", key)));
      response = searchService.bulkIndexing("table_1", documents);
      assertThat(response.getStatusLine().getStatusCode()).isPositive();

      Random rand = new Random();

      for (int i = 2; i <= NUM_TABLES; i++) {
         documents = new HashMap<>(NUM_ROWS);
         for (int j = 0; j < NUM_ROWS; j++) {
            int id = j + 1;
            String docId = id + "";
            Json json = Json.object("id", id, "table_" + (i - 1) + "_id", rand.nextInt(NUM_ROWS+1));
            documents.put(docId, json);
         }
         response = searchService.bulkIndexing("table_" + i, documents);
         assertThat(response.getStatusLine().getStatusCode()).isPositive();
      }
   }

   @Test
   public void querying() throws Exception {
      List<Map<String, Object>> results = calciteSearch.executeQuery("select * from elastic.table_1", CalciteSearch::singleColumnMapExtraction);
      assertThat(results).extracting("id").containsExactlyInAnyOrder(IntStream.range(1, NUM_ROWS + 1).boxed().toArray());
      LOG.info(results.toString());

      for (int i = 2; i <= NUM_TABLES; i++) {
         results = calciteSearch.executeQuery("select * from elastic.table_" + i, CalciteSearch::singleColumnMapExtraction);
         assertThat(results).extracting("id").containsExactlyInAnyOrder(IntStream.range(1, NUM_ROWS + 1).boxed().toArray());
         LOG.info(results.toString());
      }

      List<List<Map<String, Object>>> joinResult = calciteSearch.executeQuery(
            "select * from elastic.table_1 as t1 inner join elastic.table_2 as t2 on cast(t1._MAP['id'] AS INTEGER) = cast(t2._MAP['table_1_id'] AS INTEGER)",
            CalciteSearch::multipleColumnMapExtraction);
      assertThat(joinResult).isEmpty();
      LOG.info(joinResult.toString());
   }

   @Test
   public void views() throws Exception {
      List<Object> objects = calciteSearch.executeQuery(this::defineViews,
            "select * from table_1",
            CalciteSearch::singleColumnExtraction);

      assertThat(objects).containsExactlyInAnyOrder(IntStream.range(1, NUM_ROWS + 1).boxed().toArray());

      List<List<Object>> lists = calciteSearch.executeQuery(this::defineViews,
            "select t2.id, t2.table_1_id from table_2 as t2",
            CalciteSearch::multipleColumnExtraction);

      assertThat(lists).hasSize(NUM_ROWS);
   }

   @Test
   public void joins() throws Exception {
      StringBuilder query = new StringBuilder("SELECT * FROM table_1 AS t1 ");
      for (int i = 2; i <= NUM_TABLES; i++) {
         // INNER JOIN table_2 AS t2 ON t1.id = t2.table_1_id
         String tableAlias = "t" + i;
         query.append("inner join ");
         query.append("table_" + i);
         query.append(" as ");
         query.append(tableAlias);
         query.append(" on ");
         query.append("t" + (i-1));
         query.append(".id = ");
         query.append(tableAlias);
         query.append(".");
         query.append("table_" + (i - 1) + "_id ");
      }
      query.append("WHERE t1.id <= ");
      query.append(10);

      long a = System.currentTimeMillis();

      List<?> objects = calciteSearch.executeQuery(this::defineViews, query.toString());
      assertThat(objects).isNotNull();

      long b = System.currentTimeMillis();

      long duration = b - a;
      LOG.info("NUM_TABLES " + NUM_TABLES + " - NUM_ROWS " + NUM_ROWS + " - duration " + duration + " ms - " + objects);
   }

   @Test
   public void joins_count() throws Exception {
      StringBuilder query = new StringBuilder("SELECT count(*) FROM table_1 AS t1 ");
      for (int i = 2; i <= NUM_TABLES; i++) {
         // INNER JOIN table_2 AS t2 ON t1.id = t2.table_1_id
         String tableAlias = "t" + i;
         query.append("inner join ");
         query.append("table_" + i);
         query.append(" as ");
         query.append(tableAlias);
         query.append(" on ");
         query.append("t" + (i-1));
         query.append(".id = ");
         query.append(tableAlias);
         query.append(".");
         query.append("table_" + (i - 1) + "_id ");
      }
      query.append("WHERE t1.id <= ");
      query.append(10);

      long a = System.currentTimeMillis();

      List<?> objects = calciteSearch.executeQuery(this::defineViews, query.toString());
      assertThat(objects).isNotNull();

      long b = System.currentTimeMillis();

      long duration = b - a;
      LOG.info("NUM_TABLES " + NUM_TABLES + " - NUM_ROWS " + NUM_ROWS + " - duration " + duration + " ms <<count>> - " + objects);
   }

   public void defineViews(SchemaPlus root) {
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
