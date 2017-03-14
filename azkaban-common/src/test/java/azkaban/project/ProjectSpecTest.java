package azkaban.project;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Map;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static org.junit.Assert.*;


public class ProjectSpecTest {

  /**
   * Loads spec.yaml from test/resources and asserts properties
   *
   */
  @Test
  public void testSpecLoad() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("spec.yml").getFile());
    ProjectSpec spec = new ProjectSpecLoader().load(file);

    assertEquals("1.0", spec.getVersion());

    Map<String, URI> fetchMap = spec.getPreExec().getFetch();
    URI sampleUri = new URI("http://central.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jar");
    assertEquals(sampleUri, fetchMap.get("lib"));
    assertEquals(sampleUri, fetchMap.get("path/to/foo"));
  }
}