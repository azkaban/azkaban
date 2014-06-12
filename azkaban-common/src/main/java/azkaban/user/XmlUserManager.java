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

import java.io.File;
import java.io.IOException;
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

import azkaban.user.User.UserPermissions;
import azkaban.utils.Props;

/**
 * Xml implementation of the UserManager. Looks for the property
 * user.manager.xml.file in the azkaban properties.
 *
 * The xml to be in the following form: <azkaban-users> <user
 * username="username" password="azkaban" roles="admin" groups="azkaban"/>
 * </azkaban-users>
 */
public class XmlUserManager implements UserManager {
  private static final Logger logger = Logger.getLogger(XmlUserManager.class
      .getName());

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

  private String xmlPath;

  private HashMap<String, User> users;
  private HashMap<String, String> userPassword;
  private HashMap<String, Role> roles;
  private HashMap<String, Set<String>> groupRoles;
  private HashMap<String, Set<String>> proxyUserMap;

  /**
   * The constructor.
   *
   * @param props
   */
  public XmlUserManager(Props props) {
    xmlPath = props.getString(XML_FILE_PARAM);

    parseXMLFile();
  }

  private void parseXMLFile() {
    File file = new File(xmlPath);
    if (!file.exists()) {
      throw new IllegalArgumentException("User xml file " + xmlPath
          + " doesn't exist.");
    }

    HashMap<String, User> users = new HashMap<String, User>();
    HashMap<String, String> userPassword = new HashMap<String, String>();
    HashMap<String, Role> roles = new HashMap<String, Role>();
    HashMap<String, Set<String>> groupRoles =
        new HashMap<String, Set<String>>();
    HashMap<String, Set<String>> proxyUserMap =
        new HashMap<String, Set<String>>();

    // Creating the document builder to parse xml.
    DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IllegalArgumentException(
          "Exception while parsing user xml. Document builder not created.", e);
    }

    Document doc = null;
    try {
      doc = builder.parse(file);
    } catch (SAXException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlPath
          + ". Invalid XML.", e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while parsing " + xmlPath
          + ". Error reading file.", e);
    }

    // Only look at first item, because we should only be seeing
    // azkaban-users tag.
    NodeList tagList = doc.getChildNodes();
    Node azkabanUsers = tagList.item(0);

    NodeList azkabanUsersList = azkabanUsers.getChildNodes();
    for (int i = 0; i < azkabanUsersList.getLength(); ++i) {
      Node node = azkabanUsersList.item(i);
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

  private void parseUserTag(Node node, HashMap<String, User> users,
      HashMap<String, String> userPassword,
      HashMap<String, Set<String>> proxyUserMap) {
    NamedNodeMap userAttrMap = node.getAttributes();
    Node userNameAttr = userAttrMap.getNamedItem(USERNAME_ATTR);
    if (userNameAttr == null) {
      throw new RuntimeException("Error loading user. The '" + USERNAME_ATTR
          + "' attribute doesn't exist");
    }

    Node passwordAttr = userAttrMap.getNamedItem(PASSWORD_ATTR);
    if (passwordAttr == null) {
      throw new RuntimeException("Error loading user. The '" + PASSWORD_ATTR
          + "' attribute doesn't exist");
    }

    // Add user to the user/password map
    String username = userNameAttr.getNodeValue();
    String password = passwordAttr.getNodeValue();
    userPassword.put(username, password);
    // Add the user to the node
    User user = new User(userNameAttr.getNodeValue());
    users.put(username, user);
    logger.info("Loading user " + user.getUserId());

    Node roles = userAttrMap.getNamedItem(ROLES_ATTR);
    if (roles != null) {
      String value = roles.getNodeValue();
      String[] roleSplit = value.split("\\s*,\\s*");
      for (String role : roleSplit) {
        user.addRole(role);
      }
    }

    Node proxy = userAttrMap.getNamedItem(PROXY_ATTR);
    if (proxy != null) {
      String value = proxy.getNodeValue();
      String[] proxySplit = value.split("\\s*,\\s*");
      for (String proxyUser : proxySplit) {
        Set<String> proxySet = proxyUserMap.get(username);
        if (proxySet == null) {
          proxySet = new HashSet<String>();
          proxyUserMap.put(username, proxySet);
        }

        proxySet.add(proxyUser);
      }
    }

    Node groups = userAttrMap.getNamedItem(GROUPS_ATTR);
    if (groups != null) {
      String value = groups.getNodeValue();
      String[] groupSplit = value.split("\\s*,\\s*");
      for (String group : groupSplit) {
        user.addGroup(group);
      }
    }

    Node emailAttr = userAttrMap.getNamedItem(EMAIL_ATTR);
    if (emailAttr != null) {
      user.setEmail(emailAttr.getNodeValue());
    }
  }

  private void parseRoleTag(Node node, HashMap<String, Role> roles) {
    NamedNodeMap roleAttrMap = node.getAttributes();
    Node roleNameAttr = roleAttrMap.getNamedItem(ROLENAME_ATTR);
    if (roleNameAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'name' attribute doesn't exist");
    }
    Node permissionAttr = roleAttrMap.getNamedItem(ROLEPERMISSIONS_ATTR);
    if (permissionAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'permissions' attribute doesn't exist");
    }

    String roleName = roleNameAttr.getNodeValue();
    String permissions = permissionAttr.getNodeValue();

    String[] permissionSplit = permissions.split("\\s*,\\s*");

    Permission perm = new Permission();
    for (String permString : permissionSplit) {
      try {
        Permission.Type type = Permission.Type.valueOf(permString);
        perm.addPermission(type);
      } catch (IllegalArgumentException e) {
        logger.error("Error adding type " + permString
            + ". Permission doesn't exist.", e);
      }
    }

    Role role = new Role(roleName, perm);
    roles.put(roleName, role);
  }

  @Override
  public User getUser(String username, String password)
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
      foundPassword = userPassword.get(username);
      if (foundPassword != null) {
        user = users.get(username);
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
      public boolean hasPermission(String permission) {
        return true;
      }

      @Override
      public void addPermission(String permission) {
      }
    });
    return user;
  }

  private void resolveGroupRoles(User user) {
    for (String group : user.getGroups()) {
      Set<String> groupRoleSet = groupRoles.get(group);
      if (groupRoleSet != null) {
        for (String role : groupRoleSet) {
          user.addRole(role);
        }
      }
    }
  }

  private void parseGroupRoleTag(Node node,
      HashMap<String, Set<String>> groupRoles) {
    NamedNodeMap groupAttrMap = node.getAttributes();
    Node groupNameAttr = groupAttrMap.getNamedItem(GROUPNAME_ATTR);
    if (groupNameAttr == null) {
      throw new RuntimeException(
          "Error loading role. The role 'name' attribute doesn't exist");
    }

    String groupName = groupNameAttr.getNodeValue();
    Set<String> roleSet = new HashSet<String>();

    Node roles = groupAttrMap.getNamedItem(ROLES_ATTR);
    if (roles != null) {
      String value = roles.getNodeValue();
      String[] roleSplit = value.split("\\s*,\\s*");
      for (String role : roleSplit) {
        roleSet.add(role);
      }
    }

    groupRoles.put(groupName, roleSet);
    logger.info("Group roles " + groupName + " added.");
  }

  @Override
  public boolean validateUser(String username) {
    return users.containsKey(username);
  }

  @Override
  public Role getRole(String roleName) {
    return roles.get(roleName);
  }

  @Override
  public boolean validateGroup(String group) {
    // Return true. Validation should be added when groups are added to the xml.
    return true;
  }

  @Override
  public boolean validateProxyUser(String proxyUser, User realUser) {
    if (proxyUserMap.containsKey(realUser.getUserId())
        && proxyUserMap.get(realUser.getUserId()).contains(proxyUser)) {
      return true;
    } else {
      return false;
    }
  }
}
