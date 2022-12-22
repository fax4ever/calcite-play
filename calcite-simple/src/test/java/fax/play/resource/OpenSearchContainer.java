package fax.play.resource;

import java.util.Map;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

public class OpenSearchContainer implements AutoCloseable {

   private ElasticsearchContainer container;

   public Map<String, String> start() {
      DockerImageName dockerImageName = DockerImageName.parse("opensearchproject/opensearch:2.4.0")
            .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");

      container = new ElasticsearchContainer(dockerImageName);
      container.addEnv("discovery.type", "single-node");
      container.addEnv("plugins.security.ssl.http.enabled", "false");

      container.start();

      try {
         // After the container is started the OpenSearch server takes some time to init the security subsystem
         // TODO Find a better way to wait for it
         Thread.sleep(2000);
      } catch (InterruptedException e) {
      }

      return Map.of(
            "host", container.getHttpHostAddress(),
            "username", "admin",
            "password", "admin");
   }

   @Override
   public void close() {
      if (container != null) {
         container.stop();
      }
   }
}
