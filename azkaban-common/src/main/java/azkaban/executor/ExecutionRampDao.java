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
package azkaban.executor;

import azkaban.db.DatabaseOperator;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;


/**
 * The Hookup DB Operation for Flow Ramp
 */
@Singleton
public class ExecutionRampDao {

  private final String FAILURE_RESULT_FORMATTER = "[FAILURE] {Reason = %s, Command = %s}";
  private final String SUCCESS_RESULT_FORMATTER = "[SUCCESS] {Command = %s}";
  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutionRampDao(final DatabaseOperator dbOperator) {
    this.dbOperator = dbOperator;
  }

  /**
   * Fetch Executable Ramps
   */
  private static class FetchExecutableRamps implements ResultSetHandler<ExecutableRampMap> {

    static String FETCH_EXECUTABLE_RAMPS =
        "SELECT r.rampId, r.rampPolicy, "
            + "r.maxFailureToPause, r.maxFailureToRampDown, r.isPercentageScaleForMaxFailure, "
            + "r.startTime, r.endTime, r.lastUpdatedTime, "
            + "r.numOfTrail, r.numOfSuccess, r.numOfFailure, r.numOfIgnored, "
            + "r.isPaused, r.rampStage, r.isActive "
            + "FROM ramp r ";

    @Override
    public ExecutableRampMap handle(final ResultSet resultSet) throws SQLException {
      final ExecutableRampMap executableRampMap = ExecutableRampMap.createInstance();
      if (!resultSet.next()) {
        return executableRampMap;
      }

      do {
        executableRampMap.add(
            resultSet.getString(1),

            ExecutableRamp.builder(resultSet.getString(1), resultSet.getString(2))
                .setMetadata(
                    ExecutableRamp.Metadata.builder()
                        .setMaxFailureToPause(resultSet.getInt(3))
                        .setMaxFailureToRampDown(resultSet.getInt(4))
                        .setPercentageScaleForMaxFailure(resultSet.getBoolean(5))
                        .build()
                )
                .setState(
                    ExecutableRamp.State.builder()
                        .setStartTime(resultSet.getLong(6))
                        .setEndTime(resultSet.getLong(7))
                        .setLastUpdatedTime(resultSet.getLong(8))
                        .setNumOfTrail(resultSet.getInt(9))
                        .setNumOfSuccess(resultSet.getInt(10))
                        .setNumOfFailure(resultSet.getInt(11))
                        .setNumOfIgnored(resultSet.getInt(12))
                        .setPaused(resultSet.getBoolean(13))
                        .setRampStage(resultSet.getInt(14))
                        .setActive(resultSet.getBoolean(15))
                        .build()
                )
                .build()
        );
      } while (resultSet.next());

      return executableRampMap;
    }
  }

  public ExecutableRampMap fetchExecutableRampMap()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(
          FetchExecutableRamps.FETCH_EXECUTABLE_RAMPS,
          new FetchExecutableRamps()
      );
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error on fetching all Ramps", e);
    }
  }


  /**
   * Fetch Executable Ramp Items
   */
  private static class FetchExecutableRampItems implements ResultSetHandler<ExecutableRampItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_ITEMS =
        "SELECT rampId, dependency, rampValue "
            + "FROM ramp_items ";

    @Override
    public ExecutableRampItemsMap handle(final ResultSet resultSet) throws SQLException {
      final ExecutableRampItemsMap executableRampItemsMap = ExecutableRampItemsMap.createInstance();

      if (!resultSet.next()) {
        return executableRampItemsMap;
      }

      do {
        executableRampItemsMap.add(
            resultSet.getString(1),
            resultSet.getString(2),
            resultSet.getString(3)
        );
      } while (resultSet.next());

      return executableRampItemsMap;
    }
  }

  public ExecutableRampItemsMap fetchExecutableRampItemsMap() throws ExecutorManagerException {
    try {
      return this.dbOperator.query(
          FetchExecutableRampItems.FETCH_EXECUTABLE_RAMP_ITEMS,
          new FetchExecutableRampItems()
      );
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching active Ramp Items", e);
    }
  }

  /**
   * Fetch Rampable Dependency's default Value
   */
  private static class FetchExecutableRampDependencies implements ResultSetHandler<ExecutableRampDependencyMap> {

    static String FETCH_EXECUTABLE_RAMP_DEPENDENCIES =
        "SELECT dependency, defaultValue, jobtypes "
          + "FROM ramp_dependency ";

    @Override
    public ExecutableRampDependencyMap handle(ResultSet resultSet) throws SQLException {
      final ExecutableRampDependencyMap executableRampDependencyMap = ExecutableRampDependencyMap.createInstance();

      if (!resultSet.next()) {
        return executableRampDependencyMap;
      }

      do {
        executableRampDependencyMap
            .add(
                resultSet.getString(1),
                resultSet.getString(2),
                resultSet.getString(3)
            );
      } while (resultSet.next());

      return executableRampDependencyMap;
    }
  }

  public ExecutableRampDependencyMap fetchExecutableRampDependencyMap()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(
          FetchExecutableRampDependencies.FETCH_EXECUTABLE_RAMP_DEPENDENCIES,
          new FetchExecutableRampDependencies()
      );
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching default value list of dependencies", e);
    }
  }

  /**
   * Fetch Executable Ramp's Exceptional Flow Items
   */
  private static class FetchExecutableRampExceptionalFlowItems implements ResultSetHandler<ExecutableRampExceptionalFlowItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_FLOW_ITEMS =
        "SELECT rampId, flowId, treatment, timestamp "
            + "FROM ramp_exceptional_flow_items ";

    @Override
    public ExecutableRampExceptionalFlowItemsMap handle(ResultSet resultSet) throws SQLException {
      final ExecutableRampExceptionalFlowItemsMap executableRampExceptionalFlowItemsMap
          = ExecutableRampExceptionalFlowItemsMap.createInstance();

      if (!resultSet.next()) {
        return executableRampExceptionalFlowItemsMap;
      }

      do {
        executableRampExceptionalFlowItemsMap
            .add(
                resultSet.getString(1),
                resultSet.getString(2),
                ExecutableRampStatus.of(resultSet.getString(3)),
                resultSet.getLong(4)
            );
      } while (resultSet.next());

      return executableRampExceptionalFlowItemsMap;
    }
  }

  public ExecutableRampExceptionalFlowItemsMap fetchExecutableRampExceptionalFlowItemsMap()
    throws ExecutorManagerException {
    try {
      return this.dbOperator.query(
          FetchExecutableRampExceptionalFlowItems.FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_FLOW_ITEMS,
          new FetchExecutableRampExceptionalFlowItems()
      );
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching Executable Ramp Exceptional Flow Items", e);
    }
  }

  /**
   * Fetch Executable Ramp's Exceptional Job Items
   */
  private static class FetchExecutableRampExceptionalJobItems implements ResultSetHandler<ExecutableRampExceptionalJobItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_JOB_ITEMS =
        "SELECT rampId, flowId, jobId, treatment, timestamp "
            + "FROM ramp_exceptional_job_items ";

    @Override
    public ExecutableRampExceptionalJobItemsMap handle(ResultSet resultSet) throws SQLException {
      final ExecutableRampExceptionalJobItemsMap executableRampExceptionalJobItemsMap
          = ExecutableRampExceptionalJobItemsMap.createInstance();

      if (!resultSet.next()) {
        return executableRampExceptionalJobItemsMap;
      }

      do {
        executableRampExceptionalJobItemsMap.add(
            resultSet.getString(1),
            resultSet.getString(2),
            resultSet.getString(3),
            ExecutableRampStatus.of(resultSet.getString(4)),
            resultSet.getLong(5)
        );
      } while (resultSet.next());

      return executableRampExceptionalJobItemsMap;
    }
  }

  public ExecutableRampExceptionalJobItemsMap fetchExecutableRampExceptionalJobItemsMap()
      throws ExecutorManagerException {
    try {
      return this.dbOperator.query(
          FetchExecutableRampExceptionalJobItems.FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_JOB_ITEMS,
          new FetchExecutableRampExceptionalJobItems()
      );
    } catch (final SQLException e) {
      throw new ExecutorManagerException("Error fetching Executable Ramp Exceptional Flow Items", e);
    }
  }


  // ------------------------------------------------------------------
  // Ramp DataSets Management Section
  // ------------------------------------------------------------------
  /**
   * Generic Insert Action Function
   * @param tableName table name
   * @param actionData associated action data which include field name value pairs
   * @throws ExecutorManagerException
   */
  public void insertAction(final String tableName, Map<String, Object> actionData)
      throws ExecutorManagerException {

    if (actionData.size() == 0) {
      throw new ExecutorManagerException(
          String.format("Error on inserting into %s WITHOUT ANY DATA", tableName)
      );
    }

    try {
      if ("ramp".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("startTime", System.currentTimeMillis())
                .build()
        );
      } else if ("ramp_exceptional_flow_items".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("timestamp", System.currentTimeMillis())
                .build()
        );
      } else if ("ramp_exceptional_job_items".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("timestamp", System.currentTimeMillis())
                .build()
        );
      }

      String fieldListString = "";
      String positionListString = "";
      ArrayList<Object> values = new ArrayList<>();

      for (Entry<String, Object> element : actionData.entrySet()) {
        fieldListString += "," + element.getKey();
        positionListString += ",?";
        values.add(element.getValue());
      }

      String sqlCommand = String.format(
          "INSERT INTO %s (%s) VALUES(%s)",
          tableName,
          fieldListString.substring(1),
          positionListString.substring(1)
      );
      int rows = this.dbOperator.update(sqlCommand, values.toArray());
      if (rows <= 0) {
        throw new ExecutorManagerException(
            String.format("No record(s) is inserted into %s, with data %s", tableName, actionData)
        );
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          String.format("Error on inserting into %s, with data %s", tableName, actionData),
          e
      );
    }
  }

  /**
   * Generic Delete Action Function
   * @param tableName table name
   * @param constraints associated constraints which include field name value pairs
   * @throws ExecutorManagerException
   */
  public void deleteAction(final String tableName, Map<String, Object> constraints)
      throws ExecutorManagerException {

    if (constraints.size() == 0) {
      throw new ExecutorManagerException(
          String.format("Error on deleting from %s WITHOUT ANY CONDITIONS", tableName)
      );
    }

    try {

      String conditionListString = "";
      ArrayList<Object> values = new ArrayList<>();

      for (Entry<String, Object> element : constraints.entrySet()) {
        conditionListString += " AND " + element.getKey() + "=?";
        values.add(element.getValue());
      }

      String sqlCommand = String.format(
          "DELETE FROM %s WHERE %s",
          tableName,
          conditionListString.substring(5)
      );

      int rows = this.dbOperator.update(sqlCommand, values.toArray());
      if (rows <= 0) {
        throw new ExecutorManagerException(
            String.format("Record(s) do(es) not exist in %s, with constraints %s", tableName, constraints)
        );
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          String.format("Error on deleting from %s, with data %s", tableName, constraints),
          e
      );
    }
  }


  /**
   * Generic Update Action Function
   * @param tableName table name
   * @param actionData associated action data which include field name value pairs
   * @param constraints associated constraints which include field name value pairs
   * @throws ExecutorManagerException
   */
  public void updateAction(final String tableName, Map<String, Object> actionData, Map<String, Object> constraints)
      throws ExecutorManagerException {

    if (actionData.size() == 0 || constraints.size() == 0) {
      throw new ExecutorManagerException(
          String.format("Error on updating %s WITHOUT ANY CONDITIONS OR ANY CHANGES", tableName)
      );
    }

    try {
      if ("ramp".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("lastUpdatedTime", System.currentTimeMillis())
                .build()
        );
      } else if ("ramp_exceptional_flow_items".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("timestamp", System.currentTimeMillis())
                .build()
        );
      } else if ("ramp_exceptional_job_items".equalsIgnoreCase(tableName)) {
        actionData = adjustActionData(
            actionData,
            ImmutableMap.<String, Object>builder()
                .put("timestamp", System.currentTimeMillis())
                .build()
        );
      }

      ArrayList<Object> parameters = new ArrayList<>();

      String valueListString = "";
      for (Entry<String, Object> element : actionData.entrySet()) {
        valueListString += ", " + element.getKey() + "=?";
        parameters.add(element.getValue());
      }

      String conditionListString = "";
      for (Entry<String, Object> element : constraints.entrySet()) {
        conditionListString += " AND " + element.getKey() + "=?";
        parameters.add(element.getValue());
      }

      String sqlCommand = String.format(
          "UPDATE %s SET %s WHERE %s",
          tableName,
          valueListString.substring(2),
          conditionListString.substring(5)
      );

      int rows = this.dbOperator.update(sqlCommand, parameters.toArray());
      if (rows <= 0) {
        throw new ExecutorManagerException(
            String.format("No record(s) is updated for %s, with data %s", tableName, actionData)
        );
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          String.format("Error on updating %s, with data %s", tableName, actionData),
          e
      );
    }
  }

  /**
   * Generic data update action for Ramp DataSets
   * @param rampActionsMap list of ramp action map
   * @return result of each command
   */
  public Map<String, String> doRampActions(List<Map<String, Object>> rampActionsMap) {

    Map<String, String> result = new HashMap<>();
    for(int i = 0; i < rampActionsMap.size(); i++) {
      result.put(Integer.toString(i), doRampAction(rampActionsMap.get(i)));
    }
    return result;
  }

  private Map<String, Object> adjustActionData(Map<String, Object> actionData, Map<String, Object> defaultValues) {
    Map<String, Object> modifiedActionData = new HashMap<>();
    actionData.entrySet().stream().forEach(entry -> modifiedActionData.put(entry.getKey(), entry.getValue()));
    for (Map.Entry<String, Object> defaultValue : defaultValues.entrySet()) {
      if (!modifiedActionData.containsKey(defaultValue.getKey())) {
        modifiedActionData.put(defaultValue.getKey(), defaultValue.getValue());
      }
    }
    return modifiedActionData;
  }

  /**
   * Generic data update action for Ramp DataSets
   * @param actionDataMap ramp action map
   * @return result
   * @throws ExecutorManagerException
   */
  private String doRampAction(Map<String, Object> actionDataMap) {
    String action = (String) actionDataMap.get("action");
    String tableName = (String) actionDataMap.get("table");
    Map<String, Object> conditions =(Map<String, Object>) actionDataMap.get("conditions");
    Map<String, Object> values = (Map<String, Object>) actionDataMap.get("values");

    try {
      if ("INSERT".equalsIgnoreCase(action)) {
        insertAction(tableName, values);
      } else if ("DELETE".equalsIgnoreCase(action)) {
        deleteAction(tableName, conditions);
      } else if ("UPDATE".equalsIgnoreCase(action)) {
        updateAction(tableName, values, conditions);
      } else {
        return String.format(FAILURE_RESULT_FORMATTER, "Invalid Action", actionDataMap.toString());
      }
      return String.format(SUCCESS_RESULT_FORMATTER, actionDataMap.toString());
    } catch (ExecutorManagerException e) {
      return String.format(FAILURE_RESULT_FORMATTER, e.toString(), actionDataMap.toString());
    }
  }

  public void updateExecutableRamp(ExecutableRamp executableRamp) throws ExecutorManagerException {

    String sqlCommand = "";

    try {
      // Save all cachedNumTrail, cachedNumSuccess, cachedNumFailure, cachedNumIgnored,
      // save isPaused, endTime when it is not zero, lastUpdatedTime when it is changed.
      String ramp = executableRamp.getId();

      int cachedNumOfTrail = executableRamp.getCachedCount(ExecutableRamp.CountType.TRAIL);
      int cachedNumOfSuccess = executableRamp.getCachedCount(ExecutableRamp.CountType.SUCCESS);
      int cachedNumOfFailure = executableRamp.getCachedCount(ExecutableRamp.CountType.FAILURE);
      int cachedNumOfIgnored = executableRamp.getCachedCount(ExecutableRamp.CountType.IGNORED);
      int rampStage = executableRamp.getStage();
      long endTime = executableRamp.getEndTime();
      boolean isPaused = executableRamp.isPaused();
      long lastUpdatedTime = executableRamp.getLastUpdatedTime();

      StringBuilder sqlCommandStringBuilder = new StringBuilder();
      sqlCommandStringBuilder.append("UPDATE ramp SET ");
      sqlCommandStringBuilder.append(String.format("numOfTrail = numOfTrail + %s, ", cachedNumOfTrail));
      sqlCommandStringBuilder.append(String.format("numOfFailure = numOfFailure + %s, ", cachedNumOfFailure));
      sqlCommandStringBuilder.append(String.format("numOfSuccess = numOfSuccess + %s, ", cachedNumOfSuccess));
      sqlCommandStringBuilder.append(String.format("numOfIgnored = numOfIgnored + %s, ", cachedNumOfIgnored));
      sqlCommandStringBuilder.append(String.format("rampStage = CASE WHEN rampStage > %s THEN rampStage ELSE %s END, ", rampStage, rampStage));
      sqlCommandStringBuilder.append(String.format("endTime = CASE WHEN endTime > %s THEN endTime ELSE %s END, ", endTime, endTime));
      sqlCommandStringBuilder.append(String.format("lastUpdatedTime = CASE WHEN lastUpdatedTime > %s THEN lastUpdatedTime ELSE %s END", lastUpdatedTime, lastUpdatedTime));
      if (isPaused) {
        sqlCommandStringBuilder.append(", isPaused = true");
      }
      sqlCommandStringBuilder.append(String.format(" WHERE rampId = '%s'", ramp));

      sqlCommand = sqlCommandStringBuilder.toString();

      int rows = this.dbOperator.update(sqlCommand);
      if (rows <= 0) {
        throw new ExecutorManagerException(
            String.format("No record(s) is updated into ramp, by command [%s]", sqlCommand)
        );
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          String.format("Error on update into ramp, by command [%s]", sqlCommand),
          e
      );
    }
  }

  public void updateExecutedRampFlows(final String ramp, ExecutableRampExceptionalItems executableRampExceptionalItems)
    throws ExecutorManagerException {
    String sqlCommand = "";

    try {

      Object[][] parameters = executableRampExceptionalItems.getCachedItems().stream()
          .map(item -> {
            ArrayList<Object> object = new ArrayList<>();
            object.add(ramp);
            object.add(item.getKey());
            object.add(item.getValue().getStatus().getKey());
            object.add(item.getValue().getTimeStamp());
            return object.toArray();
          })
          .collect(Collectors.toList()).toArray(new Object[0][]);

      if (parameters.length > 0) {
        sqlCommand = "INSERT INTO ramp_exceptional_flow_items (rampId, flowId, treatment, timestamp) VALUES(?,?,?,?)";

        this.dbOperator.batch(sqlCommand, parameters);

        executableRampExceptionalItems.resetCacheFlag();
      }

    } catch (final SQLException e) {
      throw new ExecutorManagerException(
          String.format("Error on update into ramp, by command [%s]", sqlCommand),
          e
      );
    }
  }
}
