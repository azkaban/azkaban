package azkaban.user;

import static azkaban.user.Permission.Type.UPLOADPROJECTS;
import static org.assertj.core.api.Assertions.assertThat;

import azkaban.utils.TestUtils;
import org.junit.Test;


public class UserUtilsTest {

  @Test
  public void testAdminUserCanUploadProject() throws UserManagerException {
    final UserManager userManager = TestUtils.createTestXmlUserManager();
    final User testAdmin = userManager.getUser("testAdmin", "testAdmin");
    assertThat(UserUtils.hasPermissionforAction(userManager, testAdmin, UPLOADPROJECTS)).isTrue();
  }

  @Test
  public void testRegularUserCantUploadProject() {
    final UserManager userManager = TestUtils.createTestXmlUserManager();
    final User user = TestUtils.getTestUser();
    assertThat(UserUtils.hasPermissionforAction(userManager, user, UPLOADPROJECTS)).isFalse();
  }

  @Test
  public void testUserWithPermissionsCanUploadProject() throws UserManagerException {
    final UserManager userManager = TestUtils.createTestXmlUserManager();
    final User testUpload = userManager.getUser("testUpload", "testUpload");
    assertThat(UserUtils.hasPermissionforAction(userManager, testUpload, UPLOADPROJECTS)).isTrue();
  }
}
