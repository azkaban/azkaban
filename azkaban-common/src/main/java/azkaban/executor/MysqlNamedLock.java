package azkaban.executor;

import azkaban.db.DatabaseTransOperator;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 *  Util class for locking using named lock in mysql
 */

@Singleton
public class MysqlNamedLock implements ResultSetHandler<Boolean> {
  private String getLockTemplate = "SELECT GET_LOCK('%s', %s)";
  private String releaseLockTemplate = "SELECT RELEASE_LOCK('%s')";

  public boolean getLock(DatabaseTransOperator transOperator, String lockName, int lockTimeoutInSeconds) throws SQLException {
    String getLockStatement = String.format(getLockTemplate, lockName, lockTimeoutInSeconds);
    return transOperator.query(getLockStatement, this);
  }

  public boolean releaseLock(DatabaseTransOperator transOperator, String lockName) throws SQLException {
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
