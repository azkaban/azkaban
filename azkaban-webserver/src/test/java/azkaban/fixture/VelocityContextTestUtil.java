package azkaban.fixture;

import azkaban.utils.WebUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.tools.generic.EscapeTool;


/**
 * The type Velocity context test util.
 */
public class VelocityContextTestUtil {
  /**
   * Gets an instance of the velocity context
   *
   * Add the utility instances that are commonly used in many templates.
   *
   * @return the instance
   */
  public static VelocityContext getInstance() {
    VelocityContext context = new VelocityContext();
    context.put("esc", new EscapeTool());
    WebUtils utils = new WebUtils();
    context.put("utils", utils);
    return context;
  }
}