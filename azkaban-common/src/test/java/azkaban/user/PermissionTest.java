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

import static org.junit.Assert.assertTrue;

import azkaban.user.Permission.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PermissionTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testEmptyPermissionCreation() throws Exception {
    final Permission permission = new Permission();
    permission.addPermissionsByName(new String[]{});
  }

  @Test
  public void testSinglePermissionCreation() throws Exception {
    final Permission perm1 = new Permission();
    perm1.addPermissionsByName("READ");

    final Permission perm2 = new Permission();
    perm2.addPermission(Type.READ);
    info("Compare " + perm1.toString() + " and " + perm2.toString());
    assertTrue(perm1.equals(perm2));
  }

  @Test
  public void testListPermissionCreation() throws Exception {
    final Permission perm1 = new Permission();
    perm1.addPermissionsByName(new String[]{"READ", "EXECUTE"});

    final Permission perm2 = new Permission();
    perm2.addPermission(new Type[]{Type.EXECUTE, Type.READ});
    info("Compare " + perm1.toString() + " and " + perm2.toString());
    assertTrue(perm1.equals(perm2));
  }

  @Test
  public void testRemovePermission() throws Exception {
    final Permission perm1 = new Permission();
    perm1.addPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});
    perm1.removePermissions(Type.EXECUTE);

    final Permission perm2 = new Permission();
    perm2.addPermission(new Type[]{Type.READ, Type.WRITE});
    info("Compare " + perm1.toString() + " and " + perm2.toString());
    assertTrue(perm1.equals(perm2));
  }

  @Test
  public void testRemovePermissionByName() throws Exception {
    final Permission perm1 = new Permission();
    perm1.addPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});
    perm1.removePermissionsByName("EXECUTE");

    final Permission perm2 = new Permission();
    perm2.addPermission(new Type[]{Type.READ, Type.WRITE});
    info("Compare " + perm1.toString() + " and " + perm2.toString());
    assertTrue(perm1.equals(perm2));
  }

  @Test
  public void testToAndFromObject() throws Exception {
    final Permission permission = new Permission();
    permission
        .addPermissionsByName(new String[]{"READ", "EXECUTE", "WRITE"});

    final String[] array = permission.toStringArray();
    final Permission permission2 = new Permission();
    permission2.addPermissionsByName(array);
    assertTrue(permission.equals(permission2));
  }

  @Test
  public void testFlags() throws Exception {
    final Permission permission = new Permission();
    permission.addPermission(new Type[]{Type.READ, Type.WRITE});

    final int flags = permission.toFlags();
    final Permission permission2 = new Permission(flags);

    assertTrue(permission2.isPermissionSet(Type.READ));
    assertTrue(permission2.isPermissionSet(Type.WRITE));

    assertTrue(permission.equals(permission2));
  }

  /**
   * Why? because it's quicker.
   */
  public void info(final String message) {
    System.out.println(message);
  }
}
