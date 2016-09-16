package azkaban.fixture;

import java.io.StringWriter;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;


/**
 * Test utility to render a template.
 */
public class VelocityTemplateTestUtil {

  private static final String TEMPLATE_BASE_DIR = "azkaban/webapp/servlet/velocity/";

  /**
   * Render a template and return the result
   *
   * @param templateName the template name only without the .vm extension
   * @param context the context
   * @return string
   */
  public static String renderTemplate(String templateName, VelocityContext context) {
    StringWriter stringWriter = new StringWriter();
    VelocityEngine engine = new VelocityEngine();
    engine.init("src/test/resources/velocity.properties");

    engine.mergeTemplate(TEMPLATE_BASE_DIR + templateName + ".vm", "UTF-8", context, stringWriter);
    return stringWriter.getBuffer().toString();
  }
}
