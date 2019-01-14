
package azkaban.user;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.x509.X509AuthenticationInfo;
import org.apache.shiro.authc.x509.X509AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.config.Ini;
import org.apache.shiro.subject.PrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
combines ini realm (for user group and permissions) and X509Realm for authorization!

 */

public class AzkabanX509Realm extends AzkabanIniRealm /* AzkabanIniRealm /* AbstractX509Realm */{

  public AzkabanX509Realm() {
  }

  public AzkabanX509Realm(Ini ini) {
    this();
    this.processDefinitions(ini);
  }

  private static final transient Logger log = LoggerFactory.getLogger(AzkabanX509Realm.class);

  protected X509AuthenticationInfo doGetX509AuthenticationInfo(X509AuthenticationToken x509AuthenticationToken) {

    log.info(x509AuthenticationToken.toString());
    log.info(x509AuthenticationToken.getPrincipal().toString());
    log.info(x509AuthenticationToken.getCredentials().toString());
    log.info(x509AuthenticationToken.getX509Certificate().toString());

    // @TODO
    return null;
  }

  @Override
  protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
    log.info(principals.toString());
    return super.doGetAuthorizationInfo(principals);
    // return null;
  }

  protected AuthenticationInfo doGetAuthenticationInfo(
      AuthenticationToken token) throws AuthenticationException {
    return this.doGetX509AuthenticationInfo((X509AuthenticationToken) token);
  }

}
