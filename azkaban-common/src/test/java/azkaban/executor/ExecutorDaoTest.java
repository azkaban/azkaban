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
 */

package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.db.DatabaseOperator;
import azkaban.test.Utils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutorDaoTest {

  private static DatabaseOperator dbOperator;
  private ExecutorDao executorDao;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = Utils.initTestDB();
  }

  @AfterClass
  public static void destroyDB() throws Exception {
    try {
      dbOperator.update("DROP ALL OBJECTS");
      dbOperator.update("SHUTDOWN");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    this.executorDao = new ExecutorDao(dbOperator);
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("delete from executors");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  /* Test all executors fetch from empty executors */
  @Test
  public void testFetchEmptyExecutors() throws Exception {
    final List<Executor> executors = this.executorDao.fetchAllExecutors();
    assertThat(executors.size()).isEqualTo(0);
  }

  /* Test active executors fetch from empty executors */
  @Test
  public void testFetchEmptyActiveExecutors() throws Exception {
    final List<Executor> executors = this.executorDao.fetchActiveExecutors();
    assertThat(executors.size()).isEqualTo(0);
  }

  /* Test missing executor fetch with search by executor id */
  @Test
  public void testFetchMissingExecutorId() throws Exception {
    final Executor executor = this.executorDao.fetchExecutor(0);
    assertThat(executor).isEqualTo(null);
  }

  /* Test missing executor fetch with search by host:port */
  @Test
  public void testFetchMissingExecutorHostPort() throws Exception {
    final Executor executor = this.executorDao.fetchExecutor("localhost", 12345);
    assertThat(executor).isEqualTo(null);
  }

  /* Test to add duplicate executors */
  @Test
  public void testDuplicateAddExecutor() throws Exception {
    final String host = "localhost";
    final int port = 12345;
    this.executorDao.addExecutor(host, port, ExecutorTags.empty());
    assertThatThrownBy(() -> this.executorDao.addExecutor(host, port, ExecutorTags.empty()))
        .isInstanceOf(ExecutorManagerException.class)
        .hasMessageContaining("already exist");
  }

  /* Test to try update a non-existent executor */
  @Test
  public void testMissingExecutorUpdate() throws Exception {
    final Executor executor = new Executor(1, "localhost", 1234, true);
    assertThatThrownBy(() -> this.executorDao.updateExecutor(executor))
        .isInstanceOf(ExecutorManagerException.class)
        .hasMessageContaining("No executor with id");
  }

  /* Test add & fetch by Id Executors */
  @Test
  public void testSingleExecutorFetchById() throws Exception {
    final List<Executor> executors = addTestExecutors();
    for (final Executor executor : executors) {
      final Executor fetchedExecutor = this.executorDao.fetchExecutor(executor.getId());
      assertThat(executor).isEqualTo(fetchedExecutor);
    }
  }

  /* Test fetch all executors */
  @Test
  public void testFetchAllExecutors() throws Exception {
    final List<Executor> executors = addTestExecutors();
    executors.get(0).setActive(false);
    this.executorDao.updateExecutor(executors.get(0));
    final List<Executor> fetchedExecutors = this.executorDao.fetchAllExecutors();
    assertThat(executors.size()).isEqualTo(fetchedExecutors.size());
    assertThat(executors.toArray()).isEqualTo(fetchedExecutors.toArray());
  }

  /* Test fetch only active executors */
  @Test
  public void testFetchActiveExecutors() throws Exception {
    final List<Executor> executors = addTestExecutors();

    executors.get(0).setActive(true);
    this.executorDao.updateExecutor(executors.get(0));
    final List<Executor> fetchedExecutors = this.executorDao.fetchActiveExecutors();
    assertThat(executors.size()).isEqualTo(fetchedExecutors.size() + 2);
    assertThat(executors.get(0)).isEqualTo(fetchedExecutors.get(0));
  }

  /* Test add & fetch by host:port Executors */
  @Test
  public void testSingleExecutorFetchHostPort() throws Exception {
    final List<Executor> executors = addTestExecutors();
    for (final Executor executor : executors) {
      final Executor fetchedExecutor =
          this.executorDao.fetchExecutor(executor.getHost(), executor.getPort());
      assertThat(executor).isEqualTo(fetchedExecutor);
    }
  }

  /* Helper method used in methods testing jdbc interface for executors table */
  private List<Executor> addTestExecutors()
      throws ExecutorManagerException {
    final List<Executor> executors = new ArrayList<>();
    executors.add(this.executorDao.addExecutor("localhost1", 12345, ExecutorTags.empty()));
    executors.add(this.executorDao.addExecutor("localhost2", 12346, ExecutorTags.empty()));
    executors.add(this.executorDao.addExecutor("localhost1", 12347, ExecutorTags.empty()));
    return executors;
  }

  /* Test Removing Executor */
  @Test
  public void testRemovingExecutor() throws Exception {
    final Executor executor = this.executorDao
        .addExecutor("localhost1", 12345, ExecutorTags.empty());
    assertThat(executor).isNotNull();
    this.executorDao.removeExecutor("localhost1", 12345);
    final Executor fetchedExecutor = this.executorDao.fetchExecutor("localhost1", 12345);
    assertThat(fetchedExecutor).isNull();
  }

  /* Test Executor reactivation */
  @Test
  public void testExecutorActivation() throws Exception {
    final Executor executor = this.executorDao
        .addExecutor("localhost1", 12345, ExecutorTags.empty());
    assertThat(executor.isActive()).isFalse();

    executor.setActive(true);
    this.executorDao.updateExecutor(executor);
    final Executor fetchedExecutor = this.executorDao.fetchExecutor(executor.getId());
    assertThat(fetchedExecutor.isActive()).isTrue();
  }
}
