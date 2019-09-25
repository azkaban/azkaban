package azkaban.project;

import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.spi.Dependency;
import azkaban.spi.FileValidationStatus;
import azkaban.spi.Storage;
import azkaban.test.executions.ThinArchiveTestUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class JdbcDependencyManagerTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  public final String VALIDATION_KEY = "123";

  public DatabaseOperator dbOperator;
  public Storage storage;
  public JdbcDependencyManager jdbcDependencyManager;

  public Dependency depA;
  public Dependency depB;
  public Dependency depC;

  @Before
  public void setup() {
    this.dbOperator = mock(DatabaseOperator.class);
    this.storage = mock(Storage.class);

    this.jdbcDependencyManager = new JdbcDependencyManager(this.dbOperator, this.storage);

    depA = ThinArchiveTestUtils.getDepA();
    depB = ThinArchiveTestUtils.getDepB();
    depC = ThinArchiveTestUtils.getDepC();
  }

  @Test
  public void testGetValidationStatuses() throws Exception {
    // This test isn't great and does NOT verify anything about the correctness of the SQL query
    // in order to avoid brittleness...although it's actually still pretty brittle given how we have to
    // anticipate exactly how the method creates a new PreparedStatement in order to mock it.
    AzkabanDataSource dataSource = mock(AzkabanDataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(this.dbOperator.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(rs);

    // Also for some reason ResultSets return the first result at index at 1 so we set pad this with a
    // null at the beginning.
    // Columns are: file_name, file_sha1, validation_status
    Object[][] results = new Object[][] {
        null,
        {depA.getFileName(), depA.getSHA1(), FileValidationStatus.REMOVED.getValue()},
        {depC.getFileName(), depC.getSHA1(), FileValidationStatus.VALID.getValue()}
    };

    // Mock the parsing of the query result
    final AtomicInteger currResultIndex = new AtomicInteger();
    // Handle the next function
    doAnswer((Answer<Boolean>) invocation -> currResultIndex.getAndIncrement() + 1 < results.length).when(rs).next();
    // Handle file_name
    doAnswer((Answer<String>) invocation -> (String) results[currResultIndex.get()][0]).when(rs).getString(1);
    // Handle file_sha1
    doAnswer((Answer<String>) invocation -> (String) results[currResultIndex.get()][1]).when(rs).getString(2);
    // Handle validation_status
    doAnswer((Answer<Integer>) invocation -> (Integer) results[currResultIndex.get()][2]).when(rs).getInt(3);

    Map<Dependency, FileValidationStatus> expectedResult = new HashMap();
    expectedResult.put(depA, FileValidationStatus.REMOVED);
    expectedResult.put(depB, FileValidationStatus.NEW);
    expectedResult.put(depC, FileValidationStatus.VALID);

    // Assert that depB is the only dependency returned as validated
    assertEquals(expectedResult,
        this.jdbcDependencyManager.getValidationStatuses(ThinArchiveTestUtils.getDepSetABC(), VALIDATION_KEY));
  }

  @Test
  public void testUpdateValidationStatuses() throws Exception {
    // This is another VERY WEAK TEST, that basically only verifies that the function runs without exceptions
    // and calls dbOperator.batch() but does not verify anything being passed to batch (i.e. the correctness
    // of the SQL query) so as not to make this test too brittle.
    Map<Dependency, FileValidationStatus> inputStatuses = new HashMap();
    inputStatuses.put(depA, FileValidationStatus.REMOVED);
    inputStatuses.put(depC, FileValidationStatus.VALID);

    this.jdbcDependencyManager.updateValidationStatuses(inputStatuses, VALIDATION_KEY);

    verify(this.dbOperator).batch(anyString(), any());
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
