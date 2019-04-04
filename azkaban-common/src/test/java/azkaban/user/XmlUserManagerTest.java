/*
 * Copyright 2014 LinkedIn Corp.
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

import static org.junit.Assert.fail;

import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import com.google.common.io.Resources;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class XmlUserManagerTest {

  private final Props baseProps = new Props();

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Testing for when the xml path isn't set in properties.
   */
  @Test
  public void testFilePropNotSet() throws Exception {
    final Props props = new Props(this.baseProps);

    // Should throw
    try {
      final XmlUserManager manager = new XmlUserManager(props);
    } catch (final UndefinedPropertyException e) {
      return;
    }

    fail("XmlUserManager should throw an exception when the file property isn't set");
  }

  /**
   * Testing for when the xml path doesn't exist.
   */
  @Test
  public void testDoNotExist() throws Exception {
    final Props props = new Props(this.baseProps);
    props.put(XmlUserManager.XML_FILE_PARAM, "unit/test-conf/doNotExist.xml");

    try {
      final UserManager manager = new XmlUserManager(props);
    } catch (final RuntimeException e) {
      return;
    }

    fail("XmlUserManager should throw an exception when the file doesn't exist");
  }

  /**
   * Test auto reload of user XML
   */
  @Test
  public void testAutoReload() throws Exception {
    final Props props = new Props(this.baseProps);
    final URL configURL = Resources.getResource("test-conf/azkaban-users-test1.xml");
    final String path = configURL.getPath();
    props.put(XmlUserManager.XML_FILE_PARAM, path);

    UserManager manager;
    try {
      manager = new XmlUserManager(props);
    } catch (final RuntimeException e) {
      fail("Should have found the xml file");
      return;
    }

    // Get the user8 from existing XML with password == password8
    User user8 = manager.getUser("user8", "password8");

    // Modify the password for user8
    // TODO : djaiswal : Find a better way to modify XML
    List<String> lines = new ArrayList<>();
    List<String> origLines = new ArrayList<>();
    Path filePath = Paths.get(path);
    System.out.println("File modification time = " + Files.getLastModifiedTime(filePath).toString());
    for (String line : Files.readAllLines(filePath)) {
      origLines.add(line);
      if (line.contains("password8")) {
        lines.add(line.replace("password8", "passwordModified"));
      } else {
        lines.add(line);
      }
    }

    // Make sure the file gets reverted back.
    try {
      // Update the file
      Files.write(filePath, lines);
      System.out.println("File modification time after write = " + Files.getLastModifiedTime(filePath).toString());

      // Try for a minute polling every 2 seconds if the config reloaded
      try {
        for (int i = 0; i < 30; i++) {
          user8 = null;
          user8 = manager.getUser("user8", "password8");
          System.out.println("Config did not reload, sleep for 2 second, current time = " + (new java.util.Date()).toString());
          Thread.sleep(2000);
        }
      } catch (final UserManagerException e) {
        System.out.println("user8 has updated password. " + e.getMessage());
      }

      // If config reloaded, above exception would hit resulting in user8 being null
      if (user8 != null) {
        System.out.println("The config did not reload in 60 seconds");
        fail("The config did not reload in 60 seconds");
        return;
      }

      // Fetch the updated user8 info
      try {
        user8 = manager.getUser("user8", "passwordModified");
        if (!user8.getUserId().equals("user8")) {
          System.out.println("Failed to get correct user. Expected user8, got " + user8.getUserId());
          fail("Failed to get correct user. Expected user8, got " + user8.getUserId());
        }
      } catch (final UserManagerException e) {
        System.out.println("Test failed " + e.getMessage());
        fail("Test failed " + e.getMessage());
      }
    } finally {
      // Revert the file back
      Files.write(Paths.get(path), origLines);
    }
  }

  @Ignore
  @Test
  public void testBasicLoad() throws Exception {
    final Props props = new Props(this.baseProps);
    props.put(XmlUserManager.XML_FILE_PARAM,
        "unit/test-conf/azkaban-users-test1.xml");

    UserManager manager = null;
    try {
      manager = new XmlUserManager(props);
    } catch (final RuntimeException e) {
      e.printStackTrace();
      fail("XmlUserManager should've found file azkaban-users.xml");
    }

    try {
      manager.getUser("user0", null);
    } catch (final UserManagerException e) {
      System.out.println("Exception handled correctly: " + e.getMessage());
    }

    try {
      manager.getUser(null, "etw");
    } catch (final UserManagerException e) {
      System.out.println("Exception handled correctly: " + e.getMessage());
    }

    try {
      manager.getUser("user0", "user0");
    } catch (final UserManagerException e) {
      System.out.println("Exception handled correctly: " + e.getMessage());
    }

    try {
      manager.getUser("user0", "password0");
    } catch (final UserManagerException e) {
      e.printStackTrace();
      fail("XmlUserManager should've returned a user.");
    }

    final User user0 = manager.getUser("user0", "password0");
    checkUser(user0, "role0", "group0");

    final User user1 = manager.getUser("user1", "password1");
    checkUser(user1, "role0,role1", "group1,group2");

    final User user2 = manager.getUser("user2", "password2");
    checkUser(user2, "role0,role1,role2", "group1,group2,group3");

    final User user3 = manager.getUser("user3", "password3");
    checkUser(user3, "role1,role2", "group1,group2");

    final User user4 = manager.getUser("user4", "password4");
    checkUser(user4, "role1,role2", "group1,group2");

    final User user5 = manager.getUser("user5", "password5");
    checkUser(user5, "role1,role2", "group1,group2");

    final User user6 = manager.getUser("user6", "password6");
    checkUser(user6, "role3,role2", "group1,group2");

    final User user7 = manager.getUser("user7", "password7");
    checkUser(user7, "", "group1");

    final User user8 = manager.getUser("user8", "password8");
    checkUser(user8, "role3", "");

    final User user9 = manager.getUser("user9", "password9");
    checkUser(user9, "", "");
  }

  private void checkUser(final User user, final String rolesStr, final String groupsStr) {
    // Validating roles
    final HashSet<String> roleSet = new HashSet<>(user.getRoles());
    if (rolesStr.isEmpty()) {
      if (!roleSet.isEmpty()) {
        String outputRoleStr = "";
        for (final String role : roleSet) {
          outputRoleStr += role + ",";
        }
        throw new RuntimeException("Roles mismatch for " + user.getUserId()
            + ". Expected roles to be empty but got " + outputRoleStr);
      }
    } else {
      String outputRoleStr = "";
      for (final String role : roleSet) {
        outputRoleStr += role + ",";
      }

      final String[] splitRoles = rolesStr.split(",");
      final HashSet<String> expectedRoles = new HashSet<>();
      for (final String role : splitRoles) {
        if (!roleSet.contains(role)) {
          throw new RuntimeException("Roles mismatch for user "
              + user.getUserId() + " role " + role + ". Expected roles to "
              + rolesStr + " but got " + outputRoleStr);
        }
        expectedRoles.add(role);
      }

      for (final String role : roleSet) {
        if (!expectedRoles.contains(role)) {
          throw new RuntimeException("Roles mismatch for user "
              + user.getUserId() + " role " + role + ". Expected roles to "
              + rolesStr + " but got " + outputRoleStr);
        }
      }
    }

    final HashSet<String> groupSet = new HashSet<>(user.getGroups());
    if (groupsStr.isEmpty()) {
      if (!groupSet.isEmpty()) {
        String outputGroupStr = "";
        for (final String role : roleSet) {
          outputGroupStr += role + ",";
        }
        throw new RuntimeException("Roles mismatch for " + user.getUserId()
            + ". Expected roles to be empty but got " + outputGroupStr);
      }
    } else {
      String outputGroupStr = "";
      for (final String group : groupSet) {
        outputGroupStr += group + ",";
      }

      final String[] splitGroups = groupsStr.split(",");
      final HashSet<String> expectedGroups = new HashSet<>();
      for (final String group : splitGroups) {
        if (!groupSet.contains(group)) {
          throw new RuntimeException("Groups mismatch for user "
              + user.getUserId() + " group " + group + ". Expected groups to "
              + groupsStr + " but got " + outputGroupStr);
        }
        expectedGroups.add(group);
      }

      for (final String group : groupSet) {
        if (!expectedGroups.contains(group)) {
          throw new RuntimeException("Groups mismatch for user "
              + user.getUserId() + " group " + group + ". Expected groups to "
              + groupsStr + " but got " + outputGroupStr);
        }
      }
    }

  }
}
