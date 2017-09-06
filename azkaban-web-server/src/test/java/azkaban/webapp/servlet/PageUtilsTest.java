package azkaban.webapp.servlet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import azkaban.server.session.Session;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.TestUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.junit.Test;


public class PageUtilsTest {

  @Test
  public void testUploadButtonisHiddenWhenGlobalPropertyIsSet()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException,
      ClassNotFoundException, InstantiationException, IOException {
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
    final User user = TestUtils.getTestUser();
    final Session session = new Session("fake-session-id", user, "127.0.0.1");

    final VelocityEngine velocityEngine = new VelocityEngine();
    velocityEngine.init("src/test/resources/velocity.properties");
    final Page page = new Page(httpServletRequest, httpServletResponse, velocityEngine,
        "azkaban/webapp/servlet/velocity/permissionspage.vm");

    final UserManager userManager = TestUtils.createTestXmlUserManager();
    PageUtils.hideUploadButtonWhenNeeded(page, session, userManager, true);

    final Field velocityContextField = Page.class.getDeclaredField("context");
    velocityContextField.setAccessible(true);
    final VelocityContext velocityContext = (VelocityContext) velocityContextField.get(page);
    assertThat((boolean) velocityContext.get("hideUploadProject")).isTrue();
  }

  @Test
  public void testUploadButtonIsVisibleToAllWhenGlobalPropertyIsNotSet()
      throws NoSuchFieldException, IllegalAccessException {
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
    final User user = TestUtils.getTestUser();
    final Session session = new Session("fake-session-id", user, "127.0.0.1");

    final VelocityEngine velocityEngine = new VelocityEngine();
    velocityEngine.init("src/test/resources/velocity.properties");
    final Page page = new Page(httpServletRequest, httpServletResponse, velocityEngine,
        "azkaban/webapp/servlet/velocity/permissionspage.vm");

    final UserManager userManager = TestUtils.createTestXmlUserManager();
    PageUtils.hideUploadButtonWhenNeeded(page, session, userManager, false);

    final Field velocityContextField = Page.class.getDeclaredField("context");
    velocityContextField.setAccessible(true);
    final VelocityContext velocityContext = (VelocityContext) velocityContextField.get(page);
    assertThat(velocityContext.containsKey("hideUploadProject")).isFalse();
  }

  @Test
  public void testUploadButtonIsEnabledForAdminUser()
      throws NoSuchFieldException, IllegalAccessException, UserManagerException {
    final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    final HttpServletResponse httpServletResponse = mock(HttpServletResponse.class);
    final UserManager userManager = TestUtils.createTestXmlUserManager();
    final User testAdmin = userManager.getUser("testAdmin", "testAdmin");
    final Session session = new Session("fake-session-id", testAdmin, "127.0.0.1");

    final VelocityEngine velocityEngine = new VelocityEngine();
    velocityEngine.init("src/test/resources/velocity.properties");
    final Page page = new Page(httpServletRequest, httpServletResponse, velocityEngine,
        "azkaban/webapp/servlet/velocity/permissionspage.vm");

    PageUtils.hideUploadButtonWhenNeeded(page, session, userManager, false);

    final Field velocityContextField = Page.class.getDeclaredField("context");
    velocityContextField.setAccessible(true);
    final VelocityContext velocityContext = (VelocityContext) velocityContextField.get(page);
    assertThat(velocityContext.get("hideUploadProject")).isNull();
  }
}
