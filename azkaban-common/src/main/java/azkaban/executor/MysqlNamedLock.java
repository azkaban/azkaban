package azkaban.executor;

import azkaban.db.DatabaseTransOperator;
import azkaban.utils.StringUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 *  Util class for locking using named lock in mysql
 *  Note that named locks are global in MySQL, in the sense that names are locked on a
 *  server-wide basis. Multiple Azkaban instances sharing the same MySQL instance, even if using
 *  different schemas, will block each other. To avoid this, we are adding schema (database) name
 *  to the lock name below. Ref: https://dev.mysql.com/doc/refman/5.6/en/locking-functions.html
 */

@Singleton
public class MysqlNamedLock implements ResultSetHandler<Boolean> {
  private final String getLockTemplate = "SELECT GET_LOCK(CONCAT(DATABASE(), '.', '%s'), %s)";
  private final String releaseLockTemplate = "SELECT RELEASE_LOCK(CONCAT(DATABASE(), '.', '%s'))";

  public boolean getLock(DatabaseTransOperator transOperator, String lockName, int lockTimeoutInSeconds) throws SQLException {
    if (StringUtils.isEmpty(lockName)) {
      throw new IllegalArgumentException("Lock name cannot be null or empty");
    }
    String getLockStatement = String.format(getLockTemplate, lockName, lockTimeoutInSeconds);
    return transOperator.query(getLockStatement, this);
  }

  public boolean releaseLock(DatabaseTransOperator transOperator, String lockName) throws SQLException {
    if (StringUtils.isEmpty(lockName)) {
      throw new IllegalArgumentException("Lock name cannot be null or empty");
    }
    String releaseLockStatement = String.format(releaseLockTemplate, lockName);
    return transOperator.query(releaseLockStatement, this);
  }

  @Override
  public Boolean handle(final ResultSet rs) throws SQLException {
    if (!rs.next()) {
      return false;
    }
    return rs.getBoolean(1);
  }
}
