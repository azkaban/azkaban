package azkaban.webapp.servlet;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.fixture.VelocityContextTestUtil;
import azkaban.fixture.VelocityTemplateTestUtil;
import org.apache.velocity.VelocityContext;
import org.junit.Test;

/**
 * Test validates the enable/disable feature of the 'Upload' button
 */
public class ProjectPageHeaderTest {

  private static final String UPLOAD_BUTTON
      = "<button id=\"project-upload-btn\" class=\"btn btn-sm btn-primary\">"
      + "<span class=\"glyphicon glyphicon-upload\"></span> Upload </button>";

  @Test
  public void testUploadButtonIsPresent() {
    final VelocityContext context = VelocityContextTestUtil.getInstance();
    context.put("projectUploadLock", false);

    final String result =
        VelocityTemplateTestUtil.renderTemplate("projectpageheader", context);
    assertThat(VelocityTemplateTestUtil.ignoreCaseContains(result, UPLOAD_BUTTON)).isTrue();
  }

  @Test
  public void testUploadButtonIsNotPresent() {
    final VelocityContext context = VelocityContextTestUtil.getInstance();
    context.put("projectUploadLock", true);

    final String result =
        VelocityTemplateTestUtil.renderTemplate("projectpageheader", context);
    assertThat(VelocityTemplateTestUtil.ignoreCaseContains(result, UPLOAD_BUTTON)).isFalse();
  }
}
