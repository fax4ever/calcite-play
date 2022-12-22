package fax.play.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fax.play.resource.OpenSearchContainer;
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

   private static final int NUM_TABLES = 10;
   private static final int NUM_ROWS = 10_000;

   private static final Logger LOG = LoggerFactory.getLogger(JoinQueryTests.class);

   private static OpenSearchContainer searchContainer;
   private static Map<String, String> start;
   private static SearchService searchService;

   @BeforeClass
   public static void beforeAll() {
      searchContainer = new OpenSearchContainer();
      start = searchContainer.start();
      searchService = new SearchService(start.get("username"), start.get("password"), start.get("host"));
   }

   @AfterClass
   public static void afterAll() throws Exception {
      try {
         searchContainer.stop();
      } finally {
         searchService.close();
      }
   }

   @Test
   public void test() throws Exception {
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
      LOG.info(EntityUtils.toString(response.getEntity()));

      Random rand = new Random();

      for (int i = 2; i <= NUM_TABLES; i++) {
         List<Integer> finalRange = range;
         range = IntStream.rangeClosed(1, NUM_ROWS)
               .map(operand -> finalRange.get(rand.nextInt(NUM_ROWS)))
               .boxed().toList();

         documents = new HashMap<>(NUM_ROWS);
         for (int j = 0; j < NUM_ROWS; j++) {
            Json json = Json.object("id", j, "table_" + (i - 1) + "_id", range.get(i));
            documents.put(j + "", json);
         }
         response = searchService.bulkIndexing("table_" + i, documents);
         LOG.info(EntityUtils.toString(response.getEntity()));
      }

      assertThat(documents).isNotEmpty();
   }
}
