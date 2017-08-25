package azkaban.fixture;

import java.io.StringWriter;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;


/**
 * Test utility to render a template and other helper methods.
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
  public static String renderTemplate(final String templateName, final VelocityContext context) {
    final StringWriter stringWriter = new StringWriter();
    final VelocityEngine engine = new VelocityEngine();
    engine.init("src/test/resources/velocity.properties");

    engine.mergeTemplate(TEMPLATE_BASE_DIR + templateName + ".vm", "UTF-8", context, stringWriter);
    return stringWriter.getBuffer().toString();
  }

  /**
   * @param source the rendered template as a String
   * @param target the String fragment within the template
   * @return - boolean
   */
  public static boolean ignoreCaseContains(final String source, final String target) {
    final String sourceNoSpace = source.replaceAll("\\s+", "");
    final String targetNoSpace = target.replaceAll("\\s+", "");
    return sourceNoSpace.contains(targetNoSpace);
  }

}
