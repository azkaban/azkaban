package azkaban.fixture;

import azkaban.utils.TimeUtils;
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
    final VelocityContext context = new VelocityContext();
    context.put("esc", new EscapeTool());
    context.put("TimeUtils", TimeUtils.class);
    return context;
  }
}
