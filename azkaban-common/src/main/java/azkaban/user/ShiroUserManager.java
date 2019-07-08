/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.user;

import azkaban.user.User.DefaultUserPermission;
import azkaban.utils.Props;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;

/**
 * Xml implementation of the UserManager. Looks for the property user.manager.xml.file in the
 * azkaban properties.
 * <p>
 * The xml to be in the following form: <azkaban-users> <user username="username" password="azkaban"
 * shiroRoles="admin" groups="azkaban"/> </azkaban-users>
 */
public class ShiroUserManager implements UserManager {

  private static final Logger logger = Logger.getLogger(ShiroUserManager.class
      .getName());
  public final Map<String, SimpleAccount> shiroUsers;
  public final Map<String, SimpleRole> shiroRoles;
  // @TODO load proxyUserMap
  private final HashMap<String, Set<String>> proxyUserMap = new HashMap<>();

  protected final Subject currentUser;
  private AzkabanIniRealm currentRealm;
  public static final String SHIRO_FILE_PARAM = "user.manager.shiro.file";
  private final File shiroIniFile;

  /**
   * The constructor.
   */
  public ShiroUserManager(final Props props) {

    this.shiroIniFile = new File(props.getString(SHIRO_FILE_PARAM, "conf/shiro.ini"));
    DefaultSecurityManager securityManager = this.getSecurityManager();
    SecurityUtils.setSecurityManager(securityManager);

    for (Realm realm : securityManager.getRealms()) {
      logger.info("Shiro realm is " + realm.getName());
      if (realm.getName().contains("AzkabanIniRealm")) {
        this.currentRealm = (AzkabanIniRealm) realm;
      }
    }

    // this.currentRealm = (AzkabanIniRealm) SecurityUtils.getSecurityManager(); // ????
    this.currentUser = SecurityUtils.getSubject();
    if (currentRealm == null) {
      logger.error("currentRealm is null!");
    }
    this.shiroUsers = currentRealm.getUsers();
    this.shiroRoles = currentRealm.getRoles();
    // @TODO load proxyUserMap
  }

  private DefaultSecurityManager getSecurityManager() {

    assert this.shiroIniFile.exists() : "shiro.ini file " + shiroIniFile
        + " doesn't exist.";
    logger.info("Loading shiro file : " + this.shiroIniFile.toURI().toString());
    // START shiro code !! init SHIRO Steps 1. 2. 3.
    Ini myShiroIni = Ini.fromResourcePath(this.shiroIniFile.toURI().toString());
    logger.info("Shiro ini " + myShiroIni.toString());

    AzkabanIniRealm myAzkabanIniRealm = new AzkabanIniRealm(myShiroIni);
    return new DefaultSecurityManager(myAzkabanIniRealm);
  }

  @Override
  public User getUser(final String username, final String password)
      throws UserManagerException {

    if (username == null || username.trim().isEmpty()) {
      throw new UserManagerException("Username is empty.");
    } else if (password == null || password.trim().isEmpty()) {
      throw new UserManagerException("Password is empty.");
    }

    // if ( !currentUser.isAuthenticated() ) {
    //collect user principals and credentials in a gui specific manner
    //such as username/password html form, X509 certificate, OpenID, etc.
    //We'll use the username/password example here since it is the most common.
    UsernamePasswordToken token = new UsernamePasswordToken(username, password);
    //this is all you have to do to support 'remember me' (no config - built in!):
    token.setRememberMe(true);
    try {
      currentUser.login(token);
      //if no exception, that's it, we're done!
    } catch (Exception uae) {
      throw new UserManagerException(uae.getMessage());
    }
    if (currentUser.getPrincipal() == null) {
      throw new UserManagerException("User Not Found!");
    }

    // we found shiro user! now lets convert it to azkaban user!
    logger.info("User [" + currentUser.getPrincipal() + "] found successfully.");
    azkaban.user.User user = new User(currentUser.getPrincipal().toString());

    user.setPermissions(getUserPermission(currentUser));

    // find user roles and add it to azkaban user!
    for (Entry<String, SimpleRole> sr : this.shiroRoles.entrySet()) {
      if (currentUser.hasRole(sr.getValue().getName())) {
        user.addRole(sr.getValue().getName());
        logger
            .info("User [" + currentUser.getPrincipal() + "] has role:" + sr.getValue().getName());
      }
    }

    return user;
  }

  public DefaultUserPermission getUserPermission(Subject shiroUser) {
    DefaultUserPermission azPermissions = new DefaultUserPermission();

    for (azkaban.user.Permission.Type azPerm : azkaban.user.Permission.Type.values()) {
      if (shiroUser.isPermitted(azPerm.name())) {
        azPermissions.addPermission(azPerm.name());
        logger.trace(shiroUser.getPrincipal().toString() + " has permission: " + azPerm.name());
      } else {
        logger.trace(
            shiroUser.getPrincipal().toString() + " DO NOT has permission: " + azPerm.name());
      }
    }

    return azPermissions;
  }

  @Override
  public boolean validateUser(final String username) {
    boolean validate = this.shiroUsers.containsKey(username);
    logger.info("ShiroUserManager:validateUser: User " + username + " is valid ? " + validate);
    return validate;
  }

  @Override
  public Role getRole(final String roleName) {

    SimpleRole shiroRole = this.shiroRoles.get(roleName);
    azkaban.user.Permission azPermission = new azkaban.user.Permission();

    for (Permission p : shiroRole.getPermissions()) {
      try {
        final azkaban.user.Permission.Type type =
            azkaban.user.Permission.Type.valueOf((p.toString().toUpperCase()));
        azPermission.addPermission(type);
      } catch (final IllegalArgumentException e) {
        logger.error("Error adding type " + p.toString().toUpperCase()
            + ". Permission doesn't exist.", e);
      }
    }
    return new Role(roleName, azPermission);
  }

  @Override
  public boolean validateGroup(final String group) {
    // Return true. Validation should be added when groups are added to the xml.
    logger.info("ShiroUserManager:validateGroup: Group " + group + " is valid ? " + true);
    return true;
  }

  @Override
  public boolean validateProxyUser(final String proxyUser, final User realUser) {
    return this.proxyUserMap.containsKey(realUser.getUserId())
        && this.proxyUserMap.get(realUser.getUserId()).contains(proxyUser);
  }
}