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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XmlUserManagerTest {

  private static final Logger log = LoggerFactory.getLogger(XmlUserManagerTest.class);
  private final Props baseProps = new Props();
  @Mock
  private FileWatcher fileWatcher;
  @Mock
  private WatchKey watchKey;
  @Mock
  private WatchEvent<Path> watchEvent;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
  }

  /**
   * Testing for when the xml path isn't set in properties.
   */
  @Test
  public void testFilePropNotSet() throws Exception {
    final Props props = new Props(this.baseProps);

    // Should throw
    try {
      final XmlUserManager manager = new XmlUserManager(props, () -> this.fileWatcher);
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
      final UserManager manager = new XmlUserManager(props, () -> this.fileWatcher);
    } catch (final RuntimeException e) {
      return;
    }

    fail("XmlUserManager should throw an exception when the file doesn't exist");
  }

  private Path getFilePath(final String testName) throws IOException {
    final URL configURL = Resources.getResource("test-conf/azkaban-users-test1.xml");
    final String origPathStr = configURL.getPath();

    // Create a new directory and copy the file in it.
    final Path workDir = temporaryFolder.newFolder().toPath();
    // Copy the file to keep original file unmodified
    final String path = workDir.toString() + "/" + testName + ".xml";
    final Path filePath = Paths.get(path);
    Files.copy(Paths.get(origPathStr), filePath, StandardCopyOption.REPLACE_EXISTING);
    return filePath;
  }

  /**
   * Test auto reload of user XML
   */
  @Test
  public void testAutoReload() throws Exception {
    final Props props = new Props(this.baseProps);
    final Path filePath = getFilePath("testAutoReload");
    props.put(XmlUserManager.XML_FILE_PARAM, filePath.toString());

    final CountDownLatch managerLoaded = setupMocks(filePath);

    final UserManager manager = new XmlUserManager(props, () -> this.fileWatcher);

    // Get the user8 from existing XML with password == password8
    User user8 = manager.getUser("user8", "password8");

    // Modify the password for user8
    // TODO : djaiswal : Find a better way to modify XML
    final List<String> lines = new ArrayList<>();
    for (final String line : Files.readAllLines(filePath)) {
      if (line.contains("password8")) {
        lines.add(line.replace("password8", "passwordModified"));
      } else {
        lines.add(line);
      }
    }

    // Make sure the file gets reverted back.
    // Update the file
    Files.write(filePath, lines);

    managerLoaded.countDown();

    // Wait until login fails with the old password
    Awaitility.await().atMost(10L, TimeUnit.SECONDS).
        pollInterval(10L, TimeUnit.MILLISECONDS).until(
        () -> {
          User user;
          try {
            user = manager.getUser("user8", "password8");
          } catch (final UserManagerException e) {
            user = null;
          }
          return user == null;
        });

    // Assert that login succeeds with the modified password
    user8 = manager.getUser("user8", "passwordModified");
    assertEquals("user8", user8.getUserId());

    Mockito.verify(this.fileWatcher, Mockito.timeout(10_000L).atLeast(3)).take();

  }

  /**
   * Negative test auto reload of user XML
   */
  @Test
  public void testAutoReloadFail() throws Exception {
    final Props props = new Props(this.baseProps);
    final Path filePath = getFilePath("testAutoReloadFail");
    props.put(XmlUserManager.XML_FILE_PARAM, filePath.toString());

    final CountDownLatch managerLoaded = setupMocks(filePath);

    final UserManager manager;
    try {
      manager = new XmlUserManager(props, () -> this.fileWatcher);
    } catch (final RuntimeException e) {
      fail("Should have found the xml file");
      return;
    }

    // Get the user8 from existing XML with password == password8
    final User user8 = manager.getUser("user8", "password8");
    assertEquals("user8", user8.getUserId());

    // Update the file to make it empty.
    Files.write(filePath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
    managerLoaded.countDown();

    // Make sure the file gets reverted back.
    managerLoaded.await(10L, TimeUnit.SECONDS);
    assertEquals(0, managerLoaded.getCount());
    // assert that watcher.take() was still called after the failed reload
    Mockito.verify(this.fileWatcher, Mockito.timeout(10_000L).atLeast(3)).take();

    // Nothing should've changed, login should succeed as originally
    final User user = manager.getUser("user8", "password8");
    assertEquals("user8", user.getUserId());
  }

  @Ignore
  @Test
  public void testBasicLoad() throws Exception {
    final Props props = new Props(this.baseProps);
    props.put(XmlUserManager.XML_FILE_PARAM,
        "unit/test-conf/azkaban-users-test1.xml");

    UserManager manager = null;
    try {
      manager = new XmlUserManager(props, () -> this.fileWatcher);
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

  private CountDownLatch setupMocks(final Path filePath) throws IOException, InterruptedException {
    // mock registering the parent folder, but only if registered with the right args
    Mockito.doReturn(this.watchKey).when(this.fileWatcher).register(
        Paths.get(filePath.getParent().toString()));

    Mockito.doReturn(this.watchKey).when(this.fileWatcher).take();

    Mockito.doReturn(filePath).when(this.watchEvent).context();

    // thread-safe latch to allow releasing an event
    final CountDownLatch managerLoaded = new CountDownLatch(2);

    Mockito.doAnswer(invocation -> {
      // avoid busy-looping in UserUtils.java
      long sleepMillis = managerLoaded.getCount() == 0 ? 5_000L : 10L;
      if (managerLoaded.getCount() == 1) {
        // go back to returning an empty list
        managerLoaded.countDown();
        return Collections.singletonList(this.watchEvent);
      }
      Thread.sleep(sleepMillis);
      return Collections.emptyList();
    }).when(this.fileWatcher).pollEvents(this.watchKey);
    return managerLoaded;
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
