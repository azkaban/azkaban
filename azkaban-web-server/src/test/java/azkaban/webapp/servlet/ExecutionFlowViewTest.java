package azkaban.webapp.servlet;

import static org.junit.Assert.assertTrue;

import azkaban.fixture.VelocityContextTestUtil;
import azkaban.fixture.VelocityTemplateTestUtil;
import azkaban.utils.ExternalLink;
import java.util.Arrays;
import org.apache.velocity.VelocityContext;
import org.junit.Test;

/**
 * Test flow execution page.
 */
public class ExecutionFlowViewTest {

  private static final String EXTERNAL_ANALYZER_ELEMENT1 =
      "<li><a id=\"ext-link-topic1\" href=\"http://topic1.linkedin.com/\" "
          + "class=\"btn btn-info btn-sm btn-external\" type=\"button\" target=\"_blank\" "
          + "title=\"Analyze execution in Label1\">Label1</a></li>";
  private static final String EXTERNAL_ANALYZER_ELEMENT2 =
      "<li><a id=\"ext-link-topic2\" href=\"http://topic2.linkedin.com/\" "
          + "class=\"btn btn-info btn-sm btn-external disabled\" type=\"button\""
          + "target=\"_blank\" "
          + "title=\"Execution is not analyzable in Label2 at the moment.\">Label2</a></li>";
  /**
   * Test aims to check that the external analyzer button is displayed in the page.
   *
   * @throws Exception the exception
   */
    @Test
    public void testExternalAnalyzerButton() throws Exception {
      final VelocityContext context = VelocityContextTestUtil.getInstance();

      ExternalLink externalLink1 = new ExternalLink( "topic1", "Label1",
          "http://topic1.linkedin.com/", true);

      context.put("externalAnalyzers", Arrays.asList(externalLink1));

      String result =
          VelocityTemplateTestUtil.renderTemplate("executingflowpage", context);
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT1));

      ExternalLink externalLink2 = new ExternalLink( "topic2", "Label2",
          "http://topic2.linkedin.com/", false);

      context.put("externalAnalyzers", Arrays.asList(externalLink1, externalLink2));

      result =
          VelocityTemplateTestUtil.renderTemplate("executingflowpage", context);
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT1));
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT2));
    }
}
