/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.project;

import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.FileValidationStatus;
import azkaban.test.Utils;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class JdbcDependencyManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "123";

  // For finding IGNORE in a SQL query.
  private static final String SQL_IGNORE_MATCH_GROUP_REGEX = "(ignore|IGNORE)";
  private static final String SQL_IGNORE_WHOLE_MATCH_REGEX = ".*" + SQL_IGNORE_MATCH_GROUP_REGEX + ".*";

  public static DatabaseOperator dbOperator;
  public JdbcDependencyManager jdbcDependencyManager;

  public Dependency depA;
  public Dependency depB;
  public Dependency depC;

  @BeforeClass
  public static void setUp() throws Exception {
    dbOperator = spy(Utils.initTestDB());

    // THIS IS REALLY ULTRA HACKY - but necessary! :)
    // The h2 database we are using for testing does not support INSERT IGNORE but does support INSERT (obviously).
    // so we will remove the IGNORE from the query to make sure it runs properly.
    doAnswer(invocation -> {
      String queryString = (String) invocation.getArguments()[0];

      // Requires a little twisting to get the original rowsToInsert argument back. This is exactly what was passed
      // to the original method call.
      Object[][] rowsToInsert =
          Arrays.stream(Arrays.copyOfRange(invocation.getArguments(), 1, invocation.getArguments().length))
            .toArray(Object[][]::new);

      // Remove the ignore from the query string
      String fixedQueryString = queryString.replaceFirst(SQL_IGNORE_MATCH_GROUP_REGEX, "");

      // Call batch again, but this time with the fixed query string.
      return dbOperator.batch(fixedQueryString, rowsToInsert);
    }).when(dbOperator).batch(matches(SQL_IGNORE_WHOLE_MATCH_REGEX), any());
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
  public void setup() {
    this.jdbcDependencyManager = new JdbcDependencyManager(this.dbOperator);

    depA = ThinArchiveTestUtils.getDepA();
    depB = ThinArchiveTestUtils.getDepB();
    depC = ThinArchiveTestUtils.getDepC();
  }

  @After
  public void clearDB() {
    try {
      dbOperator.update("TRUNCATE TABLE validated_dependencies");
    } catch (final SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUpdateGetValidationStatuses() throws Exception {
    // --- Update the statuses of depA and depC ---
    Map<Dependency, FileValidationStatus> inputStatuses = new HashMap();
    inputStatuses.put(depA, FileValidationStatus.REMOVED);
    inputStatuses.put(depC, FileValidationStatus.VALID);

    this.jdbcDependencyManager.updateValidationStatuses(inputStatuses, VALIDATION_KEY);

    // --- Get the validation statuses of depA, depB and depC ---
    Set<Dependency> dependenciesToQuery = ThinArchiveTestUtils.getDepSetABC();
    Map<Dependency, FileValidationStatus> expectedResult = new HashMap();
    expectedResult.put(depA, FileValidationStatus.REMOVED);
    expectedResult.put(depB, FileValidationStatus.NEW);
    expectedResult.put(depC, FileValidationStatus.VALID);

    // Assert that the result is as expected.
    assertEquals(expectedResult,
        this.jdbcDependencyManager.getValidationStatuses(dependenciesToQuery, VALIDATION_KEY));
  }

  @Test
  public void testEmptyGetValidationStatuses() throws Exception {
    // We pass in an empty set, we expect to get an empty map out
    Map<Dependency, FileValidationStatus> expectedResult = new HashMap();
    assertEquals(expectedResult,
        this.jdbcDependencyManager.getValidationStatuses(new HashSet(), VALIDATION_KEY));

    // No queries should be made to DB
    verify(this.dbOperator, never()).query(anyString(), any());
  }

  @Test
  public void testEmptyUpdateValidationStatuses() throws Exception {
    // We pass in an empty map
    this.jdbcDependencyManager.updateValidationStatuses(new HashMap(), VALIDATION_KEY);

    // No updates should be made to DB
    verify(this.dbOperator, never()).batch(anyString(), any());
  }
}
