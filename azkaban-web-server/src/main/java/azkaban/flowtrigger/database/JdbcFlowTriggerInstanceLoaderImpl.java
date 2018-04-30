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

package azkaban.flowtrigger.database;

import azkaban.Constants;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.flowtrigger.CancellationCause;
import azkaban.flowtrigger.DependencyException;
import azkaban.flowtrigger.DependencyInstance;
import azkaban.flowtrigger.Status;
import azkaban.flowtrigger.TriggerInstance;
import azkaban.project.FlowLoaderUtils;
import azkaban.project.FlowTrigger;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManager;
import com.google.common.io.Files;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class JdbcFlowTriggerInstanceLoaderImpl implements FlowTriggerInstanceLoader {

  private static final Logger logger = LoggerFactory
      .getLogger(JdbcFlowTriggerInstanceLoaderImpl.class);
  private static final String[] DEPENDENCY_EXECUTIONS_COLUMNS = {"trigger_instance_id", "dep_name",
      "starttime", "endtime", "dep_status", "cancelleation_cause", "project_id", "project_version",
      "flow_id", "flow_version", "flow_exec_id"};

  private static final String DEPENDENCY_EXECUTION_TABLE = "execution_dependencies";

  private static final String INSERT_DEPENDENCY = String.format("INSERT INTO %s(%s) VALUES(%s);"
      + "", DEPENDENCY_EXECUTION_TABLE, StringUtils.join
      (DEPENDENCY_EXECUTIONS_COLUMNS, ","), String.join(",", Collections.nCopies
      (DEPENDENCY_EXECUTIONS_COLUMNS.length, "?")));

  private static final String UPDATE_DEPENDENCY_STATUS_ENDTIME_AND_CANCELLEATION_CAUSE = String
      .format
          ("UPDATE %s SET dep_status = ?, endtime = ?, cancelleation_cause  = ? WHERE trigger_instance_id = "
              + "? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_EXECUTIONS_BY_INSTANCE_ID =
      String.format("SELECT %s FROM %s WHERE trigger_instance_id = ?",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_EXECUTIONS_BY_EXEC_ID =
      String.format("SELECT %s FROM %s WHERE flow_exec_id = ?",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_ALL_PENDING_EXECUTIONS =
      String.format(
          "SELECT %s FROM %s WHERE trigger_instance_id in (SELECT trigger_instance_id FROM %s "
              + "WHERE "
              + "dep_status = %s or dep_status = %s or (dep_status = %s and "
              + "flow_exec_id = %s))",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE,
          DEPENDENCY_EXECUTION_TABLE,
          Status.RUNNING.ordinal(), Status.CANCELLING.ordinal(),
          Status.SUCCEEDED.ordinal(),
          Constants.UNASSIGNED_EXEC_ID);

  private static final String SELECT_ALL_RUNNING_EXECUTIONS =
      String.format(
          "SELECT %s FROM %s WHERE trigger_instance_id in (SELECT trigger_instance_id FROM %s "
              + "WHERE dep_status = %s or dep_status = %s)",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
          DEPENDENCY_EXECUTION_TABLE,
          DEPENDENCY_EXECUTION_TABLE,
          Status.RUNNING.ordinal(), Status.CANCELLING.ordinal());

  private static final String SELECT_RECENTLY_FINISHED = String.format(
      "SELECT execution_dependencies.trigger_instance_id,dep_name,starttime,endtime,dep_status,"
          + "cancelleation_cause,project_id,"
          + "project_version,flow_id,flow_version, flow_exec_id \n"
          + "FROM execution_dependencies JOIN (\n"
          + "SELECT trigger_instance_id FROM execution_dependencies where "
          + "trigger_instance_id not in (SELECT distinct(trigger_instance_id) FROM "
          + "execution_dependencies WHERE dep_status = %s or dep_status = %s)\n"
          + "GROUP BY trigger_instance_id ORDER BY max(endtime) DESC \n"
          + " limit %%s ) temp on execution_dependencies"
          + ".trigger_instance_id in (temp.trigger_instance_id);",
      Status.RUNNING.ordinal(),
      Status.CANCELLING.ordinal());

  private static final String SELECT_RECENT_WITH_START_AND_LENGTH = String.format("SELECT %s FROM"
          + " %s WHERE trigger_instance_id IN (\n"
          + "SELECT trigger_instance_id FROM (\n"
          + "SELECT trigger_instance_id, min(starttime) AS trigger_start_time FROM %s"
          + " WHERE project_id = ? AND flow_id = ? GROUP BY "
          + "trigger_instance_id ORDER BY trigger_start_time DESC\n"
          + "LIMIT ? OFFSET ?) AS tmp);", StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","),
      DEPENDENCY_EXECUTION_TABLE, DEPENDENCY_EXECUTION_TABLE);

  private static final String SELECT_EXECUTION_OLDER_THAN =
      String.format(
          "SELECT %s FROM %s WHERE trigger_instance_id IN (SELECT "
              + "DISTINCT(trigger_instance_id) FROM %s WHERE endtime <= ? AND endtime != 0);",
          StringUtils.join(DEPENDENCY_EXECUTIONS_COLUMNS, ","), DEPENDENCY_EXECUTION_TABLE,
          DEPENDENCY_EXECUTION_TABLE);

  private static final String DELETE_EXECUTIONS =
      String.format("DELETE FROM %s WHERE trigger_instance_id IN (?);", DEPENDENCY_EXECUTION_TABLE);

  private static final String UPDATE_DEPENDENCY_FLOW_EXEC_ID = String.format("UPDATE %s SET "
      + "flow_exec_id "
      + "= ? WHERE trigger_instance_id = ? AND dep_name = ? ;", DEPENDENCY_EXECUTION_TABLE);

  private final ProjectLoader projectLoader;
  private final DatabaseOperator dbOperator;
  private final ProjectManager projectManager;


  @Inject
  public JdbcFlowTriggerInstanceLoaderImpl(final DatabaseOperator databaseOperator,
      final ProjectLoader projectLoader, final ProjectManager projectManager) {
    this.dbOperator = databaseOperator;
    this.projectLoader = projectLoader;
    this.projectManager = projectManager;
  }

  @Override
  public Collection<TriggerInstance> getIncompleteTriggerInstances() {
    final Collection<TriggerInstance> unfinished = new ArrayList<>();
    try {
      final Collection<TriggerInstance> triggerInsts = this.dbOperator
          .query(SELECT_ALL_PENDING_EXECUTIONS,
              new TriggerInstanceHandler(SORT_MODE.SORT_ON_START_TIME_ASC));

      // select incomplete trigger instances
      for (final TriggerInstance triggerInst : triggerInsts) {
        if (!Status.isDone(triggerInst.getStatus()) || (triggerInst.getStatus() == Status.SUCCEEDED
            && triggerInst.getFlowExecId() == Constants.UNASSIGNED_EXEC_ID)) {
          unfinished.add(triggerInst);
        }
      }

      // backfilling flow trigger for unfinished trigger instances
      // dedup flow config id with a set to avoid downloading/parsing same flow file multiple times

      final Set<FlowConfigID> flowConfigIDSet = unfinished.stream()
          .map(triggerInstance -> new FlowConfigID(triggerInstance.getProject().getId(),
              triggerInstance.getProject().getVersion(), triggerInstance.getFlowId(),
              triggerInstance.getFlowVersion())).collect(Collectors.toSet());

      final Map<FlowConfigID, FlowTrigger> flowTriggers = new HashMap<>();
      for (final FlowConfigID flowConfigID : flowConfigIDSet) {
        final File tempDir = Files.createTempDir();
        try {
          final File flowFile = this.projectLoader
              .getUploadedFlowFile(flowConfigID.getProjectId(), flowConfigID.getProjectVersion(),
                  flowConfigID.getFlowId() + ".flow", flowConfigID.getFlowVersion(), tempDir);

          if (flowFile != null) {
            final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
            if (flowTrigger != null) {
              flowTriggers.put(flowConfigID, flowTrigger);
            }
          } else {
            logger.error("Unable to find flow file for " + flowConfigID);
          }
        } catch (final Exception ex) {
          logger.error("error in getting flow file", ex);
        } finally {
          FlowLoaderUtils.cleanUpDir(tempDir);
        }
      }

      for (final TriggerInstance triggerInst : unfinished) {
        triggerInst.setFlowTrigger(flowTriggers.get(new FlowConfigID(triggerInst.getProject()
            .getId(), triggerInst.getProject().getVersion(), triggerInst.getFlowId(),
            triggerInst.getFlowVersion())));
      }

    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    return unfinished;
  }

  private void handleSQLException(final SQLException ex)
      throws DependencyException {
    final String error = "exception when accessing db!";
    logger.error(error, ex);
    throw new DependencyException(error, ex);
  }

  @Override
  public void updateAssociatedFlowExecId(final TriggerInstance triggerInst) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        transOperator
            .update(UPDATE_DEPENDENCY_FLOW_EXEC_ID, triggerInst.getFlowExecId(),
                triggerInst.getId(), depInst.getDepName());
      }
      return null;
    };
    executeTransaction(insertTrigger);
  }

  private void executeUpdate(final String query, final Object... params) {
    try {
      this.dbOperator.update(query, params);
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
  }

  private void executeTransaction(final SQLTransaction<Integer> tran) {
    try {
      this.dbOperator.transaction(tran);
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
  }

  @Override
  public void uploadTriggerInstance(final TriggerInstance triggerInst) {
    final SQLTransaction<Integer> insertTrigger = transOperator -> {
      for (final DependencyInstance depInst : triggerInst.getDepInstances()) {
        transOperator
            .update(INSERT_DEPENDENCY, triggerInst.getId(), depInst.getDepName(),
                depInst.getStartTime(),
                depInst.getEndTime(),
                depInst.getStatus().ordinal(),
                depInst.getCancellationCause().ordinal(),
                triggerInst.getProject().getId(),
                triggerInst.getProject().getVersion(),
                triggerInst.getFlowId(),
                triggerInst.getFlowVersion(),
                triggerInst.getFlowExecId());
      }
      return null;
    };

    executeTransaction(insertTrigger);
  }

  @Override
  public void updateDependencyExecutionStatus(final DependencyInstance depInst) {
    executeUpdate(UPDATE_DEPENDENCY_STATUS_ENDTIME_AND_CANCELLEATION_CAUSE,
        depInst.getStatus().ordinal(),
        depInst.getEndTime(),
        depInst.getCancellationCause().ordinal(),
        depInst.getTriggerInstance().getId(),
        depInst.getDepName());
  }

  /**
   * Retrieve recently finished trigger instances, but flow trigger properties are not populated
   * into the returned trigger instances for efficiency. Flow trigger properties will be
   * retrieved only on request time.
   */
  @Override
  public Collection<TriggerInstance> getRecentlyFinished(final int limit) {
    final String query = String.format(SELECT_RECENTLY_FINISHED, limit);
    try {
      return this.dbOperator
          .query(query, new TriggerInstanceHandler(SORT_MODE.SORT_ON_START_TIME_ASC));
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    return Collections.emptyList();
  }

  @Override
  public Collection<TriggerInstance> getRunning() {
    try {
      //todo chengren311:
      // 1. add index for the execution_dependencies table to accelerate selection.
      return this.dbOperator.query(SELECT_ALL_RUNNING_EXECUTIONS, new TriggerInstanceHandler
          (SORT_MODE.SORT_ON_START_TIME_ASC));
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    return Collections.emptyList();
  }

  private void populateFlowTriggerProperties(final TriggerInstance triggerInstance) {
    if (triggerInstance != null) {
      final int projectId = triggerInstance.getProject().getId();
      final int projectVersion = triggerInstance.getProject().getVersion();
      final String flowFileName = triggerInstance.getFlowId() + ".flow";
      final int flowVersion = triggerInstance.getFlowVersion();
      final File tempDir = Files.createTempDir();
      try {
        final File flowFile = this.projectLoader
            .getUploadedFlowFile(projectId, projectVersion, flowFileName, flowVersion, tempDir);

        if (flowFile != null) {
          final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
          if (flowTrigger != null) {
            triggerInstance.setFlowTrigger(flowTrigger);
          }
        } else {
          logger.error("Unable to find flow file for " + triggerInstance);
        }
      } catch (final Exception ex) {
        logger.error("error in getting flow file", ex);
      } finally {
        FlowLoaderUtils.cleanUpDir(tempDir);
      }
    }
  }

  /**
   * Retrieve a trigger instance given a flow execution id. Flow trigger properties will
   * also be populated into the returned trigger instance. If flow exec id is -1 or -2, then
   * null will be returned.
   */
  @Override
  public TriggerInstance getTriggerInstanceByFlowExecId(final int flowExecId) {
    if (flowExecId == Constants.FAILED_EXEC_ID || flowExecId == Constants.UNASSIGNED_EXEC_ID) {
      return null;
    }
    TriggerInstance triggerInstance = null;
    try {
      final Collection<TriggerInstance> res = this.dbOperator
          .query(SELECT_EXECUTIONS_BY_EXEC_ID,
              new TriggerInstanceHandler(SORT_MODE.SORT_ON_START_TIME_ASC), flowExecId);
      triggerInstance = !res.isEmpty() ? res.iterator().next() : null;
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    populateFlowTriggerProperties(triggerInstance);
    return triggerInstance;
  }

  @Override
  /**
   * Retrieve sorted trigger instances on start time in descending order
   * given projectId, flowId, start position and length.
   * @param projectId
   * @param flowId
   * @param from starting position of the range of trigger instance to retrieve
   * @param length number of consecutive trigger instances to retrieve
   */
  public Collection<TriggerInstance> getTriggerInstances(
      final int projectId, final String flowId, final int from,
      final int length) {

    try {
      final Collection<TriggerInstance> res = this.dbOperator
          .query(SELECT_RECENT_WITH_START_AND_LENGTH, new TriggerInstanceHandler(SORT_MODE
                  .SORT_ON_START_TIME_DESC), projectId,
              flowId, length, from);
      return res;
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    return Collections.emptyList();
  }

  @Override
  public int deleteTriggerExecutionsFinishingOlderThan(final long timestamp) {
    try {
      final Collection<TriggerInstance> res = this.dbOperator
          .query(SELECT_EXECUTION_OLDER_THAN,
              new TriggerInstanceHandler(SORT_MODE.SORT_ON_START_TIME_DESC), timestamp);
      final Set<String> toBeDeleted = new HashSet<>();
      for (final TriggerInstance inst : res) {
        if ((inst.getStatus() == Status.CANCELLED || (inst.getStatus() == Status.SUCCEEDED && inst
            .getFlowExecId() != -1)) && inst.getEndTime() <= timestamp) {
          toBeDeleted.add(inst.getId());
        }
      }
      int numDeleted = 0;
      if (!toBeDeleted.isEmpty()) {
        final String ids = toBeDeleted.stream().map(s -> "'" + s + "'")
            .collect(Collectors.joining(", "));
        numDeleted = this.dbOperator.update(DELETE_EXECUTIONS.replace("?", ids));
      }
      logger.info("{} dependency instance record(s) deleted", numDeleted);
      return numDeleted;
    } catch (final SQLException ex) {
      handleSQLException(ex);
      return 0;
    }
  }

  /**
   * Retrieve a trigger instance given an instance id. Flow trigger properties will also be
   * populated into the returned trigger instance.
   */
  @Override
  public TriggerInstance getTriggerInstanceById(final String triggerInstanceId) {
    TriggerInstance triggerInstance = null;
    try {
      final Collection<TriggerInstance> res = this.dbOperator
          .query(SELECT_EXECUTIONS_BY_INSTANCE_ID,
              new TriggerInstanceHandler(SORT_MODE.SORT_ON_START_TIME_ASC),
              triggerInstanceId);
      triggerInstance = !res.isEmpty() ? res.iterator().next() : null;
    } catch (final SQLException ex) {
      handleSQLException(ex);
    }
    populateFlowTriggerProperties(triggerInstance);
    return triggerInstance;
  }

  private enum SORT_MODE {
    SORT_ON_START_TIME_DESC,
    SORT_ON_START_TIME_ASC
  }

  public static class FlowConfigID {

    private final int projectId;
    private final int projectVerison;
    private final String flowId;
    private final int flowVersion;

    public FlowConfigID(final int projectId, final int projectVerison, final String flowId,
        final int flowVersion) {
      this.projectId = projectId;
      this.projectVerison = projectVerison;
      this.flowId = flowId;
      this.flowVersion = flowVersion;
    }

    public int getProjectId() {
      return this.projectId;
    }

    public int getProjectVersion() {
      return this.projectVerison;
    }

    public String getFlowId() {
      return this.flowId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final FlowConfigID that = (FlowConfigID) o;

      return new EqualsBuilder()
          .append(this.projectId, that.projectId)
          .append(this.projectVerison, that.projectVerison)
          .append(this.flowVersion, that.flowVersion)
          .append(this.flowId, that.flowId)
          .isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(this.projectId)
          .append(this.projectVerison)
          .append(this.flowId)
          .append(this.flowVersion)
          .toHashCode();
    }

    public int getFlowVersion() {
      return this.flowVersion;
    }
  }

  private class TriggerInstanceHandler implements
      ResultSetHandler<Collection<TriggerInstance>> {

    private final SORT_MODE mode;

    public TriggerInstanceHandler(final SORT_MODE mode) {
      this.mode = mode;
    }

    @Override
    public Collection<TriggerInstance> handle(final ResultSet rs) throws SQLException {
      final Map<TriggerInstKey, List<DependencyInstance>> triggerInstMap = new HashMap<>();

      while (rs.next()) {
        final String triggerInstId = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[0]);
        final String depName = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[1]);
        final long startTime = rs.getLong(DEPENDENCY_EXECUTIONS_COLUMNS[2]);
        final long endTime = rs.getLong(DEPENDENCY_EXECUTIONS_COLUMNS[3]);
        final Status status = Status.values()[rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[4])];
        final CancellationCause cause = CancellationCause.values()[rs.getInt
            (DEPENDENCY_EXECUTIONS_COLUMNS[5])];
        final int projId = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[6]);
        final int projVersion = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[7]);
        final String flowId = rs.getString(DEPENDENCY_EXECUTIONS_COLUMNS[8]);
        final int flowVersion = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[9]);
        final Project project = JdbcFlowTriggerInstanceLoaderImpl.this.projectManager
            .getProject(projId);
        final int flowExecId = rs.getInt(DEPENDENCY_EXECUTIONS_COLUMNS[10]);

        final TriggerInstKey key = new TriggerInstKey(triggerInstId, project.getLastModifiedUser(),
            projId, projVersion, flowId, flowVersion, flowExecId, project);
        List<DependencyInstance> dependencyInstanceList = triggerInstMap.get(key);
        final DependencyInstance depInst = new DependencyInstance(depName, startTime, endTime,
            null, status, cause);
        if (dependencyInstanceList == null) {
          dependencyInstanceList = new ArrayList<>();
          triggerInstMap.put(key, dependencyInstanceList);
        }

        dependencyInstanceList.add(depInst);
      }

      final List<TriggerInstance> res = new ArrayList<>();
      for (final Map.Entry<TriggerInstKey, List<DependencyInstance>> entry : triggerInstMap
          .entrySet()) {
        res.add(new TriggerInstance(entry.getKey().triggerInstId, null, entry.getKey()
            .flowConfigID.flowId, entry.getKey().flowConfigID.flowVersion, entry.getKey()
            .submitUser, entry.getValue(), entry.getKey().flowExecId, entry.getKey().project));
      }

      if (this.mode == SORT_MODE.SORT_ON_START_TIME_ASC) {
        Collections.sort(res, Comparator.comparing(TriggerInstance::getStartTime));
      } else if (this.mode == SORT_MODE.SORT_ON_START_TIME_DESC) {
        Collections.sort(res, Comparator.comparing(TriggerInstance::getStartTime).reversed());
      }
      return res;
    }


    private class TriggerInstKey {

      String triggerInstId;
      FlowConfigID flowConfigID;
      String submitUser;
      int flowExecId;
      Project project;

      public TriggerInstKey(final String triggerInstId, final String submitUser, final int projId,
          final int projVersion, final String flowId, final int flowVerion, final int flowExecId,
          final Project project) {
        this.triggerInstId = triggerInstId;
        this.flowConfigID = new FlowConfigID(projId, projVersion, flowId, flowVerion);
        this.submitUser = submitUser;
        this.flowExecId = flowExecId;
        this.project = project;
      }

      @Override
      public boolean equals(final Object o) {
        if (this == o) {
          return true;
        }

        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        final TriggerInstKey that = (TriggerInstKey) o;

        return new EqualsBuilder()
            .append(this.triggerInstId, that.triggerInstId)
            .append(this.flowConfigID, that.flowConfigID)
            .isEquals();
      }

      @Override
      public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(this.triggerInstId)
            .append(this.flowConfigID)
            .toHashCode();
      }
    }
  }
}
