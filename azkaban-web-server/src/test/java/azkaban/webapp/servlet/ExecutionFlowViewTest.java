package azkaban.webapp.servlet;

import static org.junit.Assert.assertTrue;

import azkaban.fixture.VelocityContextTestUtil;
import azkaban.fixture.VelocityTemplateTestUtil;
import org.apache.velocity.VelocityContext;
import org.junit.Test;

/**
 * Test flow execution page.
 */
public class ExecutionFlowViewTest {

  private static final String EXTERNAL_ANALYZER_ELEMENT =
      "<li><a id=\"analyzerButton\" href=\"http://elephant.linkedin.com/\" "
          + "class=\"btn btn-info btn-sm\" type=\"button\" target=\"_blank\" "
          + "title=\"Analyze job in Dr. Elephant\">Dr. Elephant</a></li>";

  /**
   * Test aims to check that the external analyzer button is displayed in the page.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExternalAnalyzerButton() throws Exception {
    final VelocityContext context = VelocityContextTestUtil.getInstance();

    context.put("execid", 1);
    context.put("executionExternalLinkURL", "http://elephant.linkedin.com/");
    context.put("executionExternalLinkLabel", "Dr. Elephant");
    context.put("projectId", 1001);
    context.put("projectName", "user-hello-pig-azkaban");
    context.put("flowid", 27);

    final String result =
        VelocityTemplateTestUtil.renderTemplate("executingflowpage", context);
    assertTrue(VelocityTemplateTestUtil.
        ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT));
  }
}
