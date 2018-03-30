package azkaban.jobtype.connectors.jdbc;

import com.google.common.base.Optional;

/**
 * Various JDBC commands. Each operation may or may not committed depends on either implementaion class, or the one that
 * calls JdbcCommands.
 *
 */
public interface JdbcCommands {

  /**
   * Drops the table
   * @param table
   * @param database
   */
  public void dropTable(String table, Optional<String> database);

  /**
   * Truncates table.
   * @param table
   * @param database
   * @return
   */
  public Optional<Integer> truncateTable(String table, Optional<String> database);

//  /**
//   * Copies data from one table to another.
//   * @param src
//   * @param dest
//   * @param database Assumes that source and destination table is in same database.
//   * @return
//   */
//  public Optional<Integer> copyData(String src, String dest, Optional<String> database);

  /**
   * Check if table exist or not
   * @param table
   * @return
   */
  public boolean doesExist(String table, Optional<String> database);

}
