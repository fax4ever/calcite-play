package fax.play;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.calcite.test.CalciteAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fax.play.domain.Application;
import fax.play.extractor.Extractor;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JoinQueryTest {

   private static final Logger LOG = LoggerFactory.getLogger(JoinQueryTest.class);

   private Application application;

   @BeforeAll
   public void beforeAll() throws Exception {
      application = new Application();
      application.start();
   }

   @AfterAll
   public void afterAll() {
      if (application != null) {
         application.close();
      }
   }

   @Test
   public void join_usingCalciteTestFramework() throws Exception {
      Connection connection = application.createConnection();

      long a = System.currentTimeMillis();

      CalciteAssert.that()
            .with(() -> connection) // it closes the connection
            .query(createQuery(false))
            .returns(resultSet -> {
               assertThat(resultSet).isNotNull();
            });

      long b = System.currentTimeMillis();

      long durationInMillis = b - a;
      LOG.info("Query JOIN NUM_TABLES " +
            Application.NUM_TABLES + " - NUM_ROWS " + Application.NUM_ROWS + " - duration " + durationInMillis + " ms");
   }

   @Test
   public void join_analyze() {
      CalciteAssert.that()
            .with(application::createConnection)
            .query(createQuery(false))
            .queryContains(list -> {
               LOG.info("Queries executed on OpenSearch backend: " + list);
            });
   }

   @Test
   public void join_manual() throws Exception {
      try (Connection connection = application.createConnection()) {
         try (Statement statement = connection.createStatement()) {

            long a = System.currentTimeMillis();
            long b;

            try (ResultSet resultSet = statement.executeQuery(createQuery(false))) {
               b = System.currentTimeMillis();
               assertThat(resultSet).isNotNull();

               LOG.info("Manual JOIN result " + Extractor.columnExtractor(resultSet));
            }

            long durationInMillis = b - a;
            LOG.info("Manual JOIN NUM_TABLES " +
                  Application.NUM_TABLES + " - NUM_ROWS " + Application.NUM_ROWS + " - duration " + durationInMillis + " ms");
         }
      }
   }

   @Test
   public void join_explain() throws Exception {
      try (Connection connection = application.createConnection()) {
         try (Statement statement = connection.createStatement()) {

            long a = System.currentTimeMillis();
            long b;

            try (ResultSet resultSet = statement.executeQuery("explain plan for " + createQuery(false))) {
               b = System.currentTimeMillis();
               assertThat(resultSet).isNotNull();

               LOG.info("Explain JOIN result " + Extractor.columnExtractor(resultSet));
            }

            long durationInMillis = b - a;
            LOG.info("Explain JOIN NUM_TABLES " +
                  Application.NUM_TABLES + " - NUM_ROWS " + Application.NUM_ROWS + " - duration " + durationInMillis + " ms");
         }
      }
   }

   @Test
   public void join_manual_count() throws Exception {
      try (Connection connection = application.createConnection()) {
         try (Statement statement = connection.createStatement()) {

            long a = System.currentTimeMillis();
            long b;

            try (ResultSet resultSet = statement.executeQuery(createQuery(true))) {
               b = System.currentTimeMillis();
               assertThat(resultSet).isNotNull();

               LOG.info("Count JOIN result " + Extractor.columnExtractor(resultSet));
            }

            long durationInMillis = b - a;
            LOG.info("Count JOIN NUM_TABLES " +
                  Application.NUM_TABLES + " - NUM_ROWS " + Application.NUM_ROWS + " - duration " + durationInMillis + " ms");
         }
      }
   }

   private String createQuery(boolean count) {
      StringBuilder query = new StringBuilder("SELECT ");

      if (count) {
         query.append("count(*) ");
      } else {
         query.append("* ");
      }

      query.append("FROM table_1 AS t1 ");
      for (int i = 2; i <= Application.NUM_TABLES; i++) {
         // INNER JOIN table_2 AS t2 ON t1.id = t2.table_1_id
         String tableAlias = "t" + i;
         query.append("inner join ");
         query.append("table_" + i);
         query.append(" as ");
         query.append(tableAlias);
         query.append(" on ");
         query.append("t" + (i - 1));
         query.append(".id = ");
         query.append(tableAlias);
         query.append(".");
         query.append("table_" + (i - 1) + "_id ");
      }
      query.append("WHERE t1.id <= ");
      query.append(10);

      return query.toString();
   }
}
