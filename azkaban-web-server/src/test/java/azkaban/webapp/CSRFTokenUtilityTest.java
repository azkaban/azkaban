package azkaban.webapp;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.server.session.Session;
import azkaban.user.User;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class CSRFTokenUtilityTest {

  @Test
  public void assertSingleton() {
    CSRFTokenUtility csrfTokenUtility1 = CSRFTokenUtility.getCSRFTokenUtility();
    CSRFTokenUtility csrfTokenUtility2 = CSRFTokenUtility.getCSRFTokenUtility();
    assertThat(csrfTokenUtility1).isSameAs(csrfTokenUtility2).isNotNull();
  }

  @Test
  public void assertConsistentHash() {
    CSRFTokenUtility csrfTokenUtility = CSRFTokenUtility.getCSRFTokenUtility();
    Session luke = new Session(UUID.randomUUID().toString(), new User("luke"), "0.0.0.0");
    String csrfToken1 = csrfTokenUtility.getCSRFTokenFromSession(luke);
    String csrfToken2 = csrfTokenUtility.getCSRFTokenFromSession(luke);
    Assert.assertEquals(csrfToken1, csrfToken2);
  }

  @Test
  public void assertUniqueHash() {
    CSRFTokenUtility csrfTokenUtility = CSRFTokenUtility.getCSRFTokenUtility();
    Session luke = new Session(UUID.randomUUID().toString(), new User("luke"), "0.0.0.0");
    Session leia = new Session(UUID.randomUUID().toString(), new User("leia"), "0.0.0.0");
    String csrfTokenLuke = csrfTokenUtility.getCSRFTokenFromSession(luke);
    String csrfTokenLeia = csrfTokenUtility.getCSRFTokenFromSession(leia);
    Assert.assertNotEquals(csrfTokenLuke, csrfTokenLeia);
  }

}
