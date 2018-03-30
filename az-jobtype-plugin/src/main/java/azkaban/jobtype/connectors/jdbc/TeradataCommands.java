package azkaban.jobtype.connectors.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.google.common.base.Optional;

public class TeradataCommands implements JdbcCommands {
  public static final String DATABASE_TABLE_FORMAT = "%s.%s";
  private static final Logger _logger = Logger.getLogger(TeradataCommands.class);
  private static final String DROP_TABLE_STMT_FORMAT = "DROP TABLE %s";
  private static final String DELETE_TABLE_ALL_STMT_FORMAT = "DELETE %s ALL";


  private final Connection conn;

  public TeradataCommands(Connection conn) {
    this.conn = conn;
  }

  /**
   * This operation is final which means the drop table operation will be committed.
   * {@inheritDoc}
   * @see JdbcCommands#dropTable(String, Optional)
   */
  @Override
  public void dropTable(String table, Optional<String> database) {
    try {
      try (Statement stmt = conn.createStatement();) {
        String dbTbl = dbTblFormat(table, database);
        String sql = String.format(DROP_TABLE_STMT_FORMAT, dbTbl);

        _logger.info("Executing SQL: " + sql);
        stmt.execute(sql);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deleting records on specified table. Note that it is not explicitly execute commit and the commit is depends on
   * either connection setting (e.g. autocommit), or the caller to execute commit.
   *
   * {@inheritDoc}
   * @see JdbcCommands#truncateTable(String, Optional)
   */
  @Override
  public Optional<Integer> truncateTable(String table, Optional<String> database) {

    try (Statement stmt = conn.createStatement()) {
      String dbTbl = dbTblFormat(table, database);
      String sql = String.format(DELETE_TABLE_ALL_STMT_FORMAT, dbTbl);

      _logger.info("Executing SQL: " + sql);
      int deletedCount = stmt.executeUpdate(sql);
      _logger.info("All " + deletedCount + " records in " + dbTbl + " have been deleted");
      return Optional.of(deletedCount);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean doesExist(String table, Optional<String> database) {
    try (ResultSet res = conn.getMetaData().getTables(null, database.orNull(), table, new String[] { "TABLE" });) {
      return res.next();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String dbTblFormat(String table, Optional<String> database) {
    if (database.isPresent()) {
      return String.format(DATABASE_TABLE_FORMAT, database.get(), table);
    }
    return table;
  }
}
