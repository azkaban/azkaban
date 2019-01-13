package azkaban.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import azkaban.user.Permission.Type;
import azkaban.user.User.DefaultUserPermission;
import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShiroUserManagerTest {

  private final Props baseProps = new Props();
  private static final Logger logger = Logger.getLogger(ShiroUserManagerTest.class
      .getName());
  private ShiroUserManager shiroUserManager;

  @Before
  public void setUp() {
    Props props = new Props(this.baseProps);
    props.put(ShiroUserManager.SHIRO_FILE_PARAM, "src/test/resources/test-conf/shiro-test1.ini");
    this.shiroUserManager = new ShiroUserManager(props);
    logger.info(shiroUserManager.shiroUsers.toString());
    logger.info(shiroUserManager.shiroRoles.toString());
  }

  @After
  public void tearDown() {
  }

  @Test
  public void getUser() throws UserManagerException {
    assert shiroUserManager.shiroUsers.containsKey("azkaban");
    User u = shiroUserManager.getUser("user0", "password0");
    assertEquals(u.getUserId(), "user0");
  }

  @Test
  public void getUserPermission() throws UserManagerException {
    // load the user first! login!
    User u = shiroUserManager.getUser("user0", "password0");
    DefaultUserPermission p = shiroUserManager.getUserPermission(shiroUserManager.currentUser);
    logger.info(p.permissions.toString());
    assertEquals(p.permissions.size(), 1);
    assertTrue(p.permissions.contains("READ"));

    // Test 2
    u = shiroUserManager.getUser("user1", "password1");
    p = shiroUserManager.getUserPermission(shiroUserManager.currentUser);
    logger.info(p.permissions.toString());
    assertEquals(p.permissions.size(), 2);
    assertTrue(p.permissions.contains("READ"));
    assertTrue(p.permissions.contains("WRITE"));
  }

  @Test
  public void validateUser() {
    assertTrue(shiroUserManager.validateUser("user1"));
    assertTrue(shiroUserManager.validateUser("azkaban"));
    assertTrue(shiroUserManager.validateUser("metrics"));
  }

  @Test
  public void getRole() {
    // load the user first! login!
    // User u = shiroUserManager.getUser("user0","password0");
    Role r = shiroUserManager.getRole("role1");
    assertEquals(r.getName(), "role1");
    assertTrue(r.getPermission().isPermissionSet(Type.READ));
    r = shiroUserManager.getRole("role1");
    assertEquals(r.getName(), "role1");
    assertTrue(r.getPermission().isPermissionSet(Type.WRITE));
    r = shiroUserManager.getRole("admin");
    assertEquals(r.getName(), "admin");
    assertTrue(r.getPermission().isPermissionSet(Type.ADMIN));
    r = shiroUserManager.getRole("metrics");
    assertEquals(r.getName(), "metrics");
    assertTrue(r.getPermission().isPermissionSet(Type.METRICS));
  }

  @Test
  public void validateProxyUser() {
  }
}