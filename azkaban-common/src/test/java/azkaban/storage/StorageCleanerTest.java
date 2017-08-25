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
import azkaban.utils.Props;
import java.util.Arrays;
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

    when(this.databaseOperator.query(
        eq(StorageCleaner.SQL_FETCH_PVR), anyObject(), eq(TEST_PROJECT_ID)))
        .thenReturn(Arrays.asList("14/14-9.zip", "14/14-8.zip", "14/14-7.zip"));

    when(this.storage.delete("14/14-8.zip")).thenReturn(true);
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
    storageCleaner.cleanupProjectArtifacts(TEST_PROJECT_ID);

    verify(this.storage, never()).delete(anyString());
  }

  @Test
  public void testCleanup() throws Exception {
    final Props props = new Props();
    props.put(AZKABAN_STORAGE_ARTIFACT_MAX_RETENTION, 1);
    final StorageCleaner storageCleaner = new StorageCleaner(props, this.storage,
        this.databaseOperator);

    assertTrue(storageCleaner.isCleanupPermitted());
    storageCleaner.cleanupProjectArtifacts(TEST_PROJECT_ID);

    verify(this.storage, never()).delete("14/14-9.zip");
    verify(this.storage, times(1)).delete("14/14-8.zip");
    verify(this.storage, times(1)).delete("14/14-7.zip");

    verify(this.databaseOperator, never()).update(SQL_DELETE_RESOURCE_ID, "14/14-9.zip");
    verify(this.databaseOperator, times(1)).update(SQL_DELETE_RESOURCE_ID, "14/14-8.zip");
    verify(this.databaseOperator, never()).update(SQL_DELETE_RESOURCE_ID, "14/14-7.zip");
  }
}
