
package azkaban.user;

import java.util.Map;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzkabanIniRealm extends IniRealm {

  private static final transient Logger log = LoggerFactory.getLogger(AzkabanIniRealm.class);

  public AzkabanIniRealm() {
  }

  public AzkabanIniRealm(Ini ini) {
    this();
    this.processDefinitions(ini);
  }
/*
make shiro users accessible to azkaban
 */
  public Map<String, SimpleAccount> getUsers() {
    return users;
  }

  /*
  make shiro roles accessible to azkaban
   */
  public Map<String, SimpleRole> getRoles() {
    return roles;
  }

  protected void processDefinitions(Ini ini) {
    if (CollectionUtils.isEmpty(ini)) {
      log.warn("{} defined, but the ini instance is null or empty.",
          this.getClass().getSimpleName());
    } else {
      Section rolesSection = ini.getSection("roles");
      if (!CollectionUtils.isEmpty(rolesSection)) {
        log.debug("Discovered the [{}] section.  Processing...", "roles");
        this.processRoleDefinitions(rolesSection);
      }

      Section usersSection = ini.getSection("users");
      if (!CollectionUtils.isEmpty(usersSection)) {
        log.debug("Discovered the [{}] section.  Processing...", "users");
        this.processUserDefinitions(usersSection);
      } else {
        log.info(
            "{} defined, but there is no [{}] section defined.  This realm will not be populated with any users and it is assumed that they will be populated programmatically.  Users must be defined for this Realm instance to be useful.",
            this.getClass().getSimpleName(), "users");
      }

    }
  }

}
