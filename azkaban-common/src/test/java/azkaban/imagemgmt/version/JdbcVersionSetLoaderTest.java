/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.imagemgmt.version;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcVersionSetLoaderTest {

  private static DatabaseOperator dbOperator;
  private VersionSetLoader loader;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() throws IOException {
    this.loader = new JdbcVersionSetLoader(dbOperator);
  }

  /**
   * This test executes various methods and verify various cases.
   */
  @Test
  public void test() throws IOException {
    VersionSetLoader loaderSpy = Mockito.spy(this.loader);

    String testJsonString1 = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
    String testMd5Hex1 = "43966138aebfdc4438520cc5cd2aefa8";

    // Assert that it doesn't exist before
    Optional<VersionSet> versionSet = loaderSpy.getVersionSet("43966138aebfdc4438520cc5cd2aefa8");
    Assert.assertFalse(versionSet.isPresent());

    // Assert that it doesn't exist before
    versionSet = loaderSpy.getVersionSetById(1);
    Assert.assertFalse(versionSet.isPresent());

    // Try to get versionSetId which internally inserts if it doesn't exist
    int versionSetId = loaderSpy.getVersionSet(testMd5Hex1, testJsonString1).get().getVersionSetId();
    Assert.assertEquals(1, versionSetId);
    Mockito.verify(loaderSpy, Mockito.times(1))
        .insertAndGetVersionSet(Mockito.anyString(), Mockito.anyString());

    // Try to get versionSetId again which will return from the local copy
    versionSetId = loaderSpy.getVersionSet(testMd5Hex1, testJsonString1).get().getVersionSetId();
    Assert.assertEquals(1, versionSetId);
    Mockito.verify(loaderSpy, Mockito.times(1))
        .insertAndGetVersionSet(Mockito.anyString(), Mockito.anyString());

    // Try getting it again using md5
    versionSet = loaderSpy.getVersionSet("43966138aebfdc4438520cc5cd2aefa8");
    Assert.assertTrue(versionSet.isPresent());
    Assert.assertEquals(1, versionSet.get().getVersionSetId());

    // Try getting it again using Id
    versionSet = loaderSpy.getVersionSetById(1);
    Assert.assertTrue(versionSet.isPresent());
    Assert.assertEquals(1, versionSet.get().getVersionSetId());

    // Try to remove
    boolean removed = loaderSpy.deleteVersionSet("43966138aebfdc4438520cc5cd2aefa8");
    Assert.assertTrue(removed);

    // Assert that it doesn't exist now
    versionSet = loaderSpy.getVersionSet("43966138aebfdc4438520cc5cd2aefa8");
    Assert.assertFalse(versionSet.isPresent());

    // Verify that fetch doesn't return anything
    List<VersionSet> versionSets = loaderSpy.fetchAllVersionSets();
    Assert.assertTrue(versionSets.isEmpty());

    // Try to get versionSetId which internally inserts if it doesn't exist
    versionSetId = loaderSpy.getVersionSet(testMd5Hex1, testJsonString1).get().getVersionSetId();
    // This time Id will be 2 due to autoincrement field
    Assert.assertEquals(2, versionSetId);
    // Twice total number of invocations of insertAndGetVersionSetId method
    Mockito.verify(loaderSpy, Mockito.times(2))
        .insertAndGetVersionSet(Mockito.anyString(), Mockito.anyString());

    // Verify that fetch returns one record
    versionSets = loaderSpy.fetchAllVersionSets();
    Assert.assertEquals(1, versionSets.size());
  }
}
