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

  private enum RampTableFields {
    FIELD_RAMP_ID("rampId"),
    FIELD_RAMP_POLICY("rampPolicy"),
    FIELD_MAX_FAILURE_TO_PAUSE("maxFailureToPause"),
    FIELD_MAX_FAILURE_TO_RAMP_DOWN("maxFailureToRampDown"),
    FIELD_IS_PERCENTAGE_SCALE_FOR_MAX_FAILURE("isPercentageScaleForMaxFailure"),
    FIELD_START_TIME("startTime"),
    FIELD_END_TIME("endTime"),
    FIELD_LAST_UPDATED_TIME("lastUpdatedTime"),
    FIELD_NUM_OF_TRAIL("numOfTrail"),
    FIELD_NUM_OF_SUCCESS("numOfSuccess"),
    FIELD_NUM_OF_FAILURE("numOfFailure"),
    FIELD_NUM_OF_IGNORED("numOfIgnored"),
    FIELD_IS_PAUSED("isPaused"),
    FIELD_RAMP_STAGE("rampStage"),
    FIELD_IS_ACTIVE("isActive");

    final String value;

    RampTableFields(final String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  /**
   * Fetch Executable Ramps
   */
  private static class FetchExecutableRamps implements ResultSetHandler<ExecutableRampMap> {

    static String FETCH_EXECUTABLE_RAMPS = "SELECT "
        + RampTableFields.FIELD_RAMP_ID.getValue() + ", "
        + RampTableFields.FIELD_RAMP_POLICY.getValue() + ", "
        + RampTableFields.FIELD_MAX_FAILURE_TO_PAUSE.getValue() + ", "
        + RampTableFields.FIELD_MAX_FAILURE_TO_RAMP_DOWN.getValue() + ", "
        + RampTableFields.FIELD_IS_PERCENTAGE_SCALE_FOR_MAX_FAILURE.getValue() + ", "
        + RampTableFields.FIELD_START_TIME.getValue() + ", "
        + RampTableFields.FIELD_END_TIME.getValue() + ", "
        + RampTableFields.FIELD_LAST_UPDATED_TIME.getValue() + ", "
        + RampTableFields.FIELD_NUM_OF_TRAIL.getValue() + ", "
        + RampTableFields.FIELD_NUM_OF_SUCCESS.getValue() + ", "
        + RampTableFields.FIELD_NUM_OF_FAILURE.getValue() + ", "
        + RampTableFields.FIELD_NUM_OF_IGNORED.getValue() + ", "
        + RampTableFields.FIELD_IS_PAUSED.getValue() + ", "
        + RampTableFields.FIELD_RAMP_STAGE.getValue() + ", "
        + RampTableFields.FIELD_IS_ACTIVE.getValue()
        + " FROM ramp ";

    @Override
    public ExecutableRampMap handle(final ResultSet resultSet) throws SQLException {
      final ExecutableRampMap executableRampMap = ExecutableRampMap.createInstance();
      if (!resultSet.next()) {
        return executableRampMap;
      }

      do {
        executableRampMap.add(
            resultSet.getString(RampTableFields.FIELD_RAMP_ID.getValue()),

            ExecutableRamp.builder(
                resultSet.getString(RampTableFields.FIELD_RAMP_ID.getValue()),
                resultSet.getString(RampTableFields.FIELD_RAMP_POLICY.getValue()))
                .setMetadata(
                    ExecutableRamp.Metadata.builder()
                        .setMaxFailureToPause(resultSet.getInt(RampTableFields.FIELD_MAX_FAILURE_TO_PAUSE.getValue()))
                        .setMaxFailureToRampDown(resultSet.getInt(RampTableFields.FIELD_MAX_FAILURE_TO_RAMP_DOWN.getValue()))
                        .setPercentageScaleForMaxFailure(resultSet.getBoolean(RampTableFields.FIELD_IS_PERCENTAGE_SCALE_FOR_MAX_FAILURE.getValue()))
                        .build()
                )
                .setState(
                    ExecutableRamp.State.builder()
                        .setStartTime(resultSet.getLong(RampTableFields.FIELD_START_TIME.getValue()))
                        .setEndTime(resultSet.getLong(RampTableFields.FIELD_END_TIME.getValue()))
                        .setLastUpdatedTime(resultSet.getLong(RampTableFields.FIELD_LAST_UPDATED_TIME.getValue()))
                        .setNumOfTrail(resultSet.getInt(RampTableFields.FIELD_NUM_OF_TRAIL.getValue()))
                        .setNumOfSuccess(resultSet.getInt(RampTableFields.FIELD_NUM_OF_SUCCESS.getValue()))
                        .setNumOfFailure(resultSet.getInt(RampTableFields.FIELD_NUM_OF_FAILURE.getValue()))
                        .setNumOfIgnored(resultSet.getInt(RampTableFields.FIELD_NUM_OF_IGNORED.getValue()))
                        .setPaused(resultSet.getBoolean(RampTableFields.FIELD_IS_PAUSED.getValue()))
                        .setRampStage(resultSet.getInt(RampTableFields.FIELD_RAMP_STAGE.getValue()))
                        .setActive(resultSet.getBoolean(RampTableFields.FIELD_IS_ACTIVE.getValue()))
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

  private enum RampItemsTableFields {
    FIELD_RAMP_ID("rampId"),
    FIELD_DEPENDENCY("dependency"),
    FIELD_RAMP_VALUE("rampValue");

    final String value;

    RampItemsTableFields(final String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  /**
   * Fetch Executable Ramp Items
   */
  private static class FetchExecutableRampItems implements ResultSetHandler<ExecutableRampItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_ITEMS = "SELECT "
        + RampItemsTableFields.FIELD_RAMP_ID.getValue() + ", "
        + RampItemsTableFields.FIELD_DEPENDENCY.getValue() + ", "
        + RampItemsTableFields.FIELD_RAMP_VALUE.getValue()
        + " FROM ramp_items ";

    @Override
    public ExecutableRampItemsMap handle(final ResultSet resultSet) throws SQLException {
      final ExecutableRampItemsMap executableRampItemsMap = ExecutableRampItemsMap.createInstance();

      if (!resultSet.next()) {
        return executableRampItemsMap;
      }

      do {
        executableRampItemsMap.add(
            resultSet.getString(RampItemsTableFields.FIELD_RAMP_ID.getValue()),
            resultSet.getString(RampItemsTableFields.FIELD_DEPENDENCY.getValue()),
            resultSet.getString(RampItemsTableFields.FIELD_RAMP_VALUE.getValue())
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

  private enum RampDependenciesTableFields {
    FIELD_DEPENDENCY("dependency"),
    FIELD_DEFAULT_VALUE("defaultValue"),
    FIELD_JOB_TYPES("jobtypes");

    final String value;

    RampDependenciesTableFields(final String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  /**
   * Fetch Rampable Dependency's default Value
   */
  private static class FetchExecutableRampDependencies implements ResultSetHandler<ExecutableRampDependencyMap> {

    static String FETCH_EXECUTABLE_RAMP_DEPENDENCIES = "SELECT "
        + RampDependenciesTableFields.FIELD_DEPENDENCY.getValue() + ", "
        + RampDependenciesTableFields.FIELD_DEFAULT_VALUE.getValue() + ", "
        + RampDependenciesTableFields.FIELD_JOB_TYPES.getValue()
        + " FROM ramp_dependency ";

    @Override
    public ExecutableRampDependencyMap handle(ResultSet resultSet) throws SQLException {
      final ExecutableRampDependencyMap executableRampDependencyMap = ExecutableRampDependencyMap.createInstance();

      if (!resultSet.next()) {
        return executableRampDependencyMap;
      }

      do {
        executableRampDependencyMap
            .add(
                resultSet.getString(RampDependenciesTableFields.FIELD_DEPENDENCY.getValue()),
                resultSet.getString(RampDependenciesTableFields.FIELD_DEFAULT_VALUE.getValue()),
                resultSet.getString(RampDependenciesTableFields.FIELD_JOB_TYPES.getValue())
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

  private enum RampExceptionalFlowItemsTableFields {
    FIELD_RAMP_ID("rampId"),
    FIELD_FLOW_ID("flowId"),
    FIELD_TREATMENT("treatment"),
    FIELD_TIME_STAMP("timestamp");

    final String value;

    RampExceptionalFlowItemsTableFields(final String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  /**
   * Fetch Executable Ramp's Exceptional Flow Items
   */
  private static class FetchExecutableRampExceptionalFlowItems implements ResultSetHandler<ExecutableRampExceptionalFlowItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_FLOW_ITEMS = "SELECT "
        + RampExceptionalFlowItemsTableFields.FIELD_RAMP_ID.getValue() + ", "
        + RampExceptionalFlowItemsTableFields.FIELD_FLOW_ID.getValue() + ", "
        + RampExceptionalFlowItemsTableFields.FIELD_TREATMENT.getValue() + ", "
        + RampExceptionalFlowItemsTableFields.FIELD_TIME_STAMP.getValue()
        + " FROM ramp_exceptional_flow_items ";

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

  private enum RampExceptionalJobItemsTableFields {
    FIELD_RAMP_ID("rampId"),
    FIELD_FLOW_ID("flowId"),
    FIELD_JOB_ID("jobId"),
    FIELD_TREATMENT("treatment"),
    FIELD_TIME_STAMP("timestamp");

    final String value;

    RampExceptionalJobItemsTableFields(final String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  /**
   * Fetch Executable Ramp's Exceptional Job Items
   */
  private static class FetchExecutableRampExceptionalJobItems implements ResultSetHandler<ExecutableRampExceptionalJobItemsMap> {

    static String FETCH_EXECUTABLE_RAMP_EXCEPTIONAL_JOB_ITEMS = "SELECT "
        + RampExceptionalJobItemsTableFields.FIELD_RAMP_ID.getValue() + ", "
        + RampExceptionalJobItemsTableFields.FIELD_FLOW_ID.getValue() + ", "
        + RampExceptionalJobItemsTableFields.FIELD_JOB_ID.getValue() + ", "
        + RampExceptionalJobItemsTableFields.FIELD_TREATMENT.getValue() + ", "
        + RampExceptionalJobItemsTableFields.FIELD_TIME_STAMP.getValue()
        + " FROM ramp_exceptional_job_items ";

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
