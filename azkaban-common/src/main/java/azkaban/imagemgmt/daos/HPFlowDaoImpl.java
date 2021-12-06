package azkaban.imagemgmt.daos;

import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.executor.ExecutableFlow;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtDaoException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import static azkaban.imagemgmt.utils.ErroCodeConstants.*;


/**
 * Dao implementation for managing high priority flows
 */
@Singleton
public class HPFlowDaoImpl implements HPFlowDao {
  private static final Logger log = Logger.getLogger(HPFlowDaoImpl.class);

  private final DatabaseOperator databaseOperator;

  private static String INSERT_HP_FLOW_OWNER =
      "INSERT into hp_flow_owners (name, created_on, created_by, "
      + "modified_on, modified_by ) "
      + "values (?, ?, ?, ?, ?)";

  private static String INSERT_HP_FLOWS =
      "INSERT into hp_flows (flow_id, created_on, created_by) "
      + "values (?, ?, ?)";

  @Inject
  public HPFlowDaoImpl(final DatabaseOperator databaseOperator) {
    this.databaseOperator = databaseOperator;
  }

  /**
   * Returns if the provided user has high priority flow management access.
   * @param userId : owner of high priority flows.
   * @return
   */
  @Override
  public boolean isHPFlowOwner(final String userId) {
    final FetchHPFlowOwnership fetchHPFlowOwnership = new FetchHPFlowOwnership();
    try {
      final String returnedName = this.databaseOperator
          .query(FetchHPFlowOwnership.FETCH_HP_OWNER_BY_NAME,
              fetchHPFlowOwnership, userId.toLowerCase());
      log.info(String.format(
          "HPFlowDao : Returned user id %s for given user id %s", returnedName, userId));
      return returnedName.equalsIgnoreCase(userId);
    } catch (final SQLException e) {
      log.error(FetchHPFlowOwnership.FETCH_HP_OWNER_BY_NAME + " failed.", e);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR, String.format(
          "Unable to fetch HP flow ownership for %s.", userId));
    }
  }

  /**
   * Adds a list of flows into high priority flows db.
   * @param flowIds : list of flows.
   * @param userId : userId of owner who is adding the HP flows.
   * @return
   */
  @Override
  public int addHPFlows(final List<String> flowIds, final String userId) {
    // Create the transaction block
    final SQLTransaction<Integer> insertAllFlows = transOperator -> {
      // Insert each flow in its own row with same userId and created time.
      final Timestamp currTimeStamp = Timestamp.valueOf(LocalDateTime.now());
      int count = 0;
      for (final String flowId : flowIds) {
        transOperator.update(INSERT_HP_FLOWS, flowId, currTimeStamp, userId);
        count++;
      }
      transOperator.getConnection().commit();
      return count;
    };

    int count = 0;
    try {
      count = this.databaseOperator.transaction(insertAllFlows);
      if (count < 1) {
        log.error("Exception while adding HP flows");
        throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST,
            "Exception while adding HP flows");
      }
      log.info("Added the HP flows by user " + userId);
      return count;
    } catch (final SQLException e) {
      log.error("Unable to add HP flows", e);
      String errorMessage = "";
      if (e.getErrorCode() == SQL_ERROR_CODE_DUPLICATE_ENTRY) {
        errorMessage = "Reason: One or more flows already exists.";
      } else {
        errorMessage = "Reason: ";
      }
      throw new ImageMgmtDaoException(ErrorCode.BAD_REQUEST, "Exception while "
          + "adding HP flows. " + errorMessage);
    }
  }

  /**
   * Checks if the provided flow is high priority.
   * @param flow : Given a flow, finds if it is a high priority flow.
   * @return
   */
  @Override
  public boolean isHPFlow(final ExecutableFlow flow) {
    if (null == flow) {
      return false;
    }
    final FetchHPFlow fetchHPFlow = new FetchHPFlow();
    final String FQFN = flow.getProjectName() + "." + flow.getFlowId();
    try {
      final String returnedFlowId = this.databaseOperator
          .query(FetchHPFlow.SELECT_HP_FLOW, fetchHPFlow, FQFN);
      log.info("HPFlowDao : Found high priority flow "
          + returnedFlowId == null ? "" : returnedFlowId);
      return returnedFlowId.equals(FQFN);
    } catch (final SQLException e) {
      log.error(FetchHPFlow.SELECT_HP_FLOW + " failed.", e);
      throw new ImageMgmtDaoException(ErrorCode.INTERNAL_SERVER_ERROR, String.format(
          "Unable to fetch high priority flow info for flow %s.", FQFN));
    }
  }

  /**
   * ResultSetHandler implementation class for fetching high priority flow ownership
   */
  public static class FetchHPFlowOwnership implements ResultSetHandler<String> {
    private static final String FETCH_HP_OWNER_BY_NAME =
        "SELECT name FROM hp_flow_owners where lower(name) = ?";

    @Override
    public String handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }
      return rs.getString("name");
    }
  }

  /**
   * ResultSetHandler implementation class for fetching high priority flow
   */
  public static class FetchHPFlow implements ResultSetHandler<String> {

    private static String SELECT_HP_FLOW =
        "SELECT count(*) from hp_flows where flow_id = ?";

    @Override
    public String handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }
      return rs.getString("flow_id");
    }
  }
}
