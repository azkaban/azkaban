/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.storage;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION;
import static azkaban.storage.StorageCleaner.SQL_DELETE_RESOURCE_ID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Storage;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StorageCleanerTest {

  public static final int TEST_PROJECT_ID = 14;

  private Storage storage;
  private DatabaseOperator databaseOperator;

  @Before
  public void setUp() throws Exception {
    this.databaseOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);

    List<Pair<String, Integer>> project_versions = Arrays.asList(
        new Pair<>("14/14-8.zip", 5), // Different versions can have the same resource id!
        new Pair<>("14/14-10.zip", 4),
        new Pair<>("14/14-9.zip", 3),
        new Pair<>("14/14-8.zip", 2),
        new Pair<>("14/14-7.zip", 1));
    when(this.databaseOperator.query(
        eq(StorageCleaner.SQL_FETCH_PVR), anyObject(), eq(TEST_PROJECT_ID)))
        .thenReturn(project_versions);

    when(this.storage.delete("14/14-10.zip")).thenReturn(true);
    when(this.storage.delete("14/14-9.zip")).thenReturn(true);
    // This one shouldn't be deleted because it's shared with latest version
    when(this.storage.delete("14/14-8.zip")).thenThrow(Exception.class);
    when(this.storage.delete("14/14-7.zip")).thenReturn(false);
    when(this.databaseOperator.update(any(), anyVararg())).thenReturn(1);
  }

  /**
   * test default behavior. By default no artifacts should be cleaned up.
   */
  @Test
  public void testNoCleanupCase1() throws Exception {
    final StorageCleaner storageCleaner = new StorageCleaner(new Props(), this.storage,
        this.databaseOperator);

    assertFalse(storageCleaner.isCleanupPermitted());
  }

  @Test
  public void testNoCleanupCase2() throws Exception {
    final Props props = new Props();
    props.put(AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION, 10);
    final StorageCleaner storageCleaner = new StorageCleaner(props, this.storage,
        this.databaseOperator);

    assertTrue(storageCleaner.isCleanupPermitted());
    storageCleaner.cleanupProjectArtifacts(TEST_PROJECT_ID, new ArrayList<>());

    verify(this.storage, never()).delete(anyString());
  }

  @Test
  public void testCleanup() throws Exception {
    final Props props = new Props();
    props.put(AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION, 1);
    final StorageCleaner storageCleaner = new StorageCleaner(props, this.storage,
        this.databaseOperator);

    assertTrue(storageCleaner.isCleanupPermitted());
    storageCleaner.cleanupProjectArtifacts(TEST_PROJECT_ID, Arrays.asList(3));

    // Verify that latest artifact cannot be deleted
    verify(this.storage, never()).delete("14/14-8.zip");
    // Verify that artifact with project version 3 cannot be deleted
    verify(this.storage, never()).delete("14/14-9.zip");
    // Verify that remaining artifacts should be deleted
    verify(this.storage, times(1)).delete("14/14-10.zip");
    verify(this.storage, times(1)).delete("14/14-7.zip");

    verify(this.databaseOperator, never()).update(SQL_DELETE_RESOURCE_ID, "14/14-8.zip");
    verify(this.databaseOperator, never()).update(SQL_DELETE_RESOURCE_ID, "14/14-9.zip");
    verify(this.databaseOperator, times(1)).update(SQL_DELETE_RESOURCE_ID, "14/14-10.zip");
    // Verify there was no db update due to previous deletion failure
    verify(this.databaseOperator, never()).update(SQL_DELETE_RESOURCE_ID, "14/14-7.zip");
  }
}
