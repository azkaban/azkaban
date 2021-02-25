package azkaban.webapp.servlet;

import static org.junit.Assert.assertTrue;

import azkaban.fixture.VelocityContextTestUtil;
import azkaban.fixture.VelocityTemplateTestUtil;
import azkaban.utils.ExternalAnalyzer;
import java.util.Arrays;
import org.apache.velocity.VelocityContext;
import org.junit.Test;

/**
 * Test flow execution page.
 */
public class ExecutionFlowViewTest {

  private static final String EXTERNAL_ANALYZER_ELEMENT1 =
      "<li><a id=\"analyzerButtontopic1\" href=\"http://topic1.linkedin.com/\" "
          + "class=\"btn btn-info btn-sm btn-external\" type=\"button\" target=\"_blank\" "
          + "title=\"Analyze job in Label1\">Label1</a></li>";
  private static final String EXTERNAL_ANALYZER_ELEMENT2 =
      "<li><a id=\"analyzerButtontopic2\" href=\"http://topic2.linkedin.com/\" "
          + "class=\"btn btn-info btn-sm btn-external\" type=\"button\" disabled target=\"_blank\" "
          + "title=\"Analyze job Label2 is disabled currently as link is not reachable. If Label2"
          + " is applicable for this execution, you can try again later.\">Label2</a></li>";
  /**
   * Test aims to check that the external analyzer button is displayed in the page.
   *
   * @throws Exception the exception
   */
    @Test
    public void testExternalAnalyzerButton() throws Exception {
      final VelocityContext context = VelocityContextTestUtil.getInstance();

      ExternalAnalyzer externalAnalyzer1 = new ExternalAnalyzer( "topic1",
          "Label1",
          "http://topic1.linkedin.com/", true);

      context.put("externalAnalyzers", Arrays.asList(externalAnalyzer1));

      String result =
          VelocityTemplateTestUtil.renderTemplate("executingflowpage", context);
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT1));

      ExternalAnalyzer externalAnalyzer2 = new ExternalAnalyzer( "topic2",
          "Label2",
          "http://topic2.linkedin.com/", false);

      context.put("externalAnalyzers", Arrays.asList(externalAnalyzer1, externalAnalyzer2));

      result =
          VelocityTemplateTestUtil.renderTemplate("executingflowpage", context);
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT1));
      assertTrue(VelocityTemplateTestUtil.
          ignoreCaseContains(result, EXTERNAL_ANALYZER_ELEMENT2));
    }
}
