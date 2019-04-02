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

import azkaban.user.User.UserPermissions;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Xml implementation of the UserManager. Looks for the property user.manager.xml.file in the
 * azkaban properties.
 *
 * The xml to be in the following form: <azkaban-users> <user username="username" password="azkaban"
 * roles="admin" groups="azkaban"/> </azkaban-users>
 */
public class XmlUserManager implements UserManager {

  public static final String XML_FILE_PARAM = "user.manager.xml.file";
  public static final String AZKABAN_USERS_TAG = "azkaban-users";
  public static final String USER_TAG = "user";
  public static final String ROLE_TAG = "role";
  public static final String GROUP_TAG = "group";
  public static final String ROLENAME_ATTR = "name";
  public static final String ROLEPERMISSIONS_ATTR = "permissions";
  public static final String USERNAME_ATTR = "username";
  public static final String PASSWORD_ATTR = "password";
  public static final String EMAIL_ATTR = "email";
  public static final String ROLES_ATTR = "roles";
  public static final String PROXY_ATTR = "proxy";
  public static final String GROUPS_ATTR = "groups";
  public static final String GROUPNAME_ATTR = "name";
  private static final Logger logger = Logger.getLogger(XmlUserManager.class
      .getName());
  private final String xmlPath;

  private HashMap<String, User> users;
  private HashMap<String, String> userPassword;
  private HashMap<String, Role> roles;
  private HashMap<String, Set<String>> groupRoles;
  private HashMap<String, Set<String>> proxyUserMap;

  /**
   * The constructor.
   */
  public XmlUserManager(final Props props) {
    this.xmlPath = props.getString(XML_FILE_PARAM);

    parseXMLFile();

    // Create a thread which listens to any change in user config file and
    // reloads it.
    UserUtils.setupWatch(this.xmlPath, this::parseXMLFile);
  }

  private void parseXMLFile() {
    final File file = new File(this.xmlPath);
    if (!file.exists()) {
      throw new IllegalArgumentException("User xml file " + this.xmlPath
          + " doesn't exist.");
    }

    final HashMap<String, User> users = new HashMap<>();
    final HashMap<String, String> userPassword = new HashMap<>();
    final HashMap<String, Role> roles = new HashMap<>();
    final HashMap<String, Set<String>> groupRoles =
        new HashMap<>();
    final HashMap<String, Set<String>> proxyUserMap =
        new HashMap<>();

    // Creating the document builder to parse xml.
    final DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = docBuilderFactory.newDocumentBuilder();
    } catch (final ParserConfigurationException e) {
      throw new IllegalArgumentException(
          "Exception while parsing user xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (final SAXException e) {
      throw new IllegalArgumentException("Exception while parsing " + this.xmlPath
          + ". Invalid XML.", e);
    } catch (final IOException e) {
      throw new IllegalArgumentException("Exception while parsing " + this.xmlPath
          + ". Error reading file.", e);
    }

    // Only look at first item, because we should only be seeing
    // azkaban-users tag.
    final NodeList tagList = doc.getChildNodes();
    final Node azkabanUsers = tagList.item(0);

    final NodeList azkabanUsersList = azkabanUsers.getChildNodes();
    for (int i = 0; i < azkabanUsersList.getLength(); ++i) {
      final Node node = azkabanUsersList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        if (node.getNodeName().equals(USER_TAG)) {
          parseUserTag(node, users, userPassword, proxyUserMap);
        } else if (node.getNodeName().equals(ROLE_TAG)) {
          parseRoleTag(node, roles);
        } else if (node.getNodeName().equals(GROUP_TAG)) {
          parseGroupRoleTag(node, groupRoles);
        }
      }
    }

    // Synchronize the swap. Similarly, the gets are synchronized to this.
    synchronized (this) {
      this.users = users;
      this.userPassword = userPassword;
      this.roles = roles;
      this.proxyUserMap = proxyUserMap;
      this.groupRoles = groupRoles;
    }
  }

  private void parseUserTag(final Node node, final HashMap<String, User> users,
      final HashMap<String, String> userPassword,
      final HashMap<String, Set<String>> proxyUserMap) {
    final NamedNodeMap userAttrMap = node.getAttributes();
    final Node userNameAttr = userAttrMap.getNamedItem(USERNAME_ATTR);
    if (userNameAttr == null) {
      throw new RuntimeException("Error loading user. The '" + USERNAME_ATTR
          + "' attribute doesn't exist");
    }

    final Node passwordAttr = userAttrMap.getNamedItem(PASSWORD_ATTR);
    if (passwordAttr == null) {
      throw new RuntimeException("Error loading user. The '" + PASSWORD_ATTR
          + "' attribute doesn't exist");
    }

    // Add user to the user/password map
    final String username = userNameAttr.getNodeValue();
    final String password = passwordAttr.getNodeValue();
    userPassword.put(username, password);
    // Add the user to the node
    final User user = new User(userNameAttr.getNodeValue());
    users.put(username, user);
    logger.info("Loading user " + user.getUserId());

    final Node roles = userAttrMap.getNamedItem(ROLES_ATTR);
    if (roles != null) {
      final String value = roles.getNodeValue();
      final String[] roleSplit = value.split("\\s*,\\s*");
      for (final String role : roleSplit) {
        user.addRole(role);
      }
    }

    final Node proxy = userAttrMap.getNamedItem(PROXY_ATTR);
    if (proxy != null) {
      final String value = proxy.getNodeValue();
      final String[] proxySplit = value.split("\\s*,\\s*");
      for (final String proxyUser : proxySplit) {
        Set<String> proxySet = proxyUserMap.get(username);
        if (proxySet == null) {
          proxySet = new HashSet<>();
          proxyUserMap.put(username, proxySet);
        }

        proxySet.add(proxyUser);
      }
    }

    final Node groups = userAttrMap.getNamedItem(GROUPS_ATTR);
    if (groups != null) {
      final String value = groups.getNodeValue();
      final String[] groupSplit = value.split("\\s*,\\s*");
      for (final String group : groupSplit) {
        user.addGroup(group);
      }
    }

    final Node emailAttr = userAttrMap.getNamedItem(EMAIL_ATTR);
    if (emailAttr != null) {
      user.setEmail(emailAttr.getNodeValue());
    }
  }

  private void parseRoleTag(final Node node, final HashMap<String, Role> roles) {
    final NamedNodeMap roleAttrMap = node.getAttributes();
    final Node roleNameAttr = roleAttrMap.getNamedItem(ROLENAME_ATTR);
    if (roleNameAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'name' attribute doesn't exist");
    }
    final Node permissionAttr = roleAttrMap.getNamedItem(ROLEPERMISSIONS_ATTR);
    if (permissionAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'permissions' attribute doesn't exist");
    }

    final String roleName = roleNameAttr.getNodeValue();
    final String permissions = permissionAttr.getNodeValue();

    final String[] permissionSplit = permissions.split("\\s*,\\s*");

    final Permission perm = new Permission();
    for (final String permString : permissionSplit) {
      try {
        final Permission.Type type = Permission.Type.valueOf(permString);
        perm.addPermission(type);
      } catch (final IllegalArgumentException e) {
        logger.error("Error adding type " + permString
            + ". Permission doesn't exist.", e);
      }
    }

    final Role role = new Role(roleName, perm);
    roles.put(roleName, role);
  }

  @Override
  public User getUser(final String username, final String password)
      throws UserManagerException {
    if (username == null || username.trim().isEmpty()) {
      throw new UserManagerException("Username is empty.");
    } else if (password == null || password.trim().isEmpty()) {
      throw new UserManagerException("Password is empty.");
    }

    // Minimize the synchronization of the get. Shouldn't matter if it
    // doesn't exist.
    String foundPassword = null;
    User user = null;
    synchronized (this) {
      foundPassword = this.userPassword.get(username);
      if (foundPassword != null) {
        user = this.users.get(username);
      }
    }

    if (foundPassword == null || !foundPassword.equals(password)) {
      throw new UserManagerException("Username/Password not found.");
    }
    // Once it gets to this point, no exception has been thrown. User
    // shoudn't be
    // null, but adding this check for if user and user/password hash tables
    // go
    // out of sync.
    if (user == null) {
      throw new UserManagerException("Internal error: User not found.");
    }

    // Add all the roles the group has to the user
    resolveGroupRoles(user);
    user.setPermissions(new UserPermissions() {
      @Override
      public boolean hasPermission(final String permission) {
        return true;
      }

      @Override
      public void addPermission(final String permission) {
      }
    });
    return user;
  }

  private void resolveGroupRoles(final User user) {
    for (final String group : user.getGroups()) {
      final Set<String> groupRoleSet = this.groupRoles.get(group);
      if (groupRoleSet != null) {
        for (final String role : groupRoleSet) {
          user.addRole(role);
        }
      }
    }
  }

  private void parseGroupRoleTag(final Node node,
      final HashMap<String, Set<String>> groupRoles) {
    final NamedNodeMap groupAttrMap = node.getAttributes();
    final Node groupNameAttr = groupAttrMap.getNamedItem(GROUPNAME_ATTR);
    if (groupNameAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'name' attribute doesn't exist");
    }

    final String groupName = groupNameAttr.getNodeValue();
    final Set<String> roleSet = new HashSet<>();

    final Node roles = groupAttrMap.getNamedItem(ROLES_ATTR);
    if (roles != null) {
      final String value = roles.getNodeValue();
      final String[] roleSplit = value.split("\\s*,\\s*");
      for (final String role : roleSplit) {
        roleSet.add(role);
      }
    }

    groupRoles.put(groupName, roleSet);
    logger.info("Group roles " + groupName + " added.");
  }

  @Override
  public boolean validateUser(final String username) {
    return this.users.containsKey(username);
  }

  @Override
  public Role getRole(final String roleName) {
    return this.roles.get(roleName);
  }

  @Override
  public boolean validateGroup(final String group) {
    // Return true. Validation should be added when groups are added to the xml.
    return true;
  }

  @Override
  public boolean validateProxyUser(final String proxyUser, final User realUser) {
    if (this.proxyUserMap.containsKey(realUser.getUserId())
        && this.proxyUserMap.get(realUser.getUserId()).contains(proxyUser)) {
      return true;
    } else {
      return false;
    }
  }
}
