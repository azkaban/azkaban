package azkaban.executor;

import azkaban.db.DatabaseOperator;
import azkaban.project.Dataset;
import azkaban.project.DatasetUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

@Singleton
public class ExecutionJobDatasetDao {

  private static final Logger logger = Logger.getLogger(ExecutionJobDatasetDao.class);
  private final DatabaseOperator dbOperator;

  @Inject
  public ExecutionJobDatasetDao(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  /**
   * Inserts each dataset as separate row in execution_job_dataset
   *
   * @param datasetMap  map having dataset string with raw path as key and dataset string with
   *                    resolved path as value
   * @param datasetType dataset type - input or output
   * @param node        object containing the job metadata
   * @throws ExecutorManagerException
   */
  public void addExecutionJobDataset(final Map<String, String> datasetMap, final String datasetType,
      final ExecutableNode node) throws ExecutorManagerException {

    String sqlCommand = "";
    final int execId = node.getParentFlow().getExecutionId();
    final String jobId = node.getId();
    final int attempt = node.getAttempt();

    try {
      final Object[][] parameters = datasetMap.entrySet().stream()
          .map(dataset -> {
            ArrayList<Object> object = new ArrayList<>();
            object.add(execId);
            object.add(jobId);
            object.add(attempt);
            object.add(datasetType);
            object.add(dataset.getKey());
            object.add(dataset.getValue());
            object.add(dataset.getValue());
            return object.toArray();
          })
          .collect(Collectors.toList()).toArray(new Object[0][]);

      if (parameters.length > 0) {
        sqlCommand = "INSERT INTO execution_job_dataset (exec_id, job_id, attempt, dataset_type, "
            + "raw_dataset, resolved_dataset) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE resolved_dataset = ?";

        this.dbOperator.batch(sqlCommand, parameters);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format(
          "Error on inserting into execution_job_dataset, by command [%s]", sqlCommand), e);
    }
  }

  /**
   * Fetches all output datasets for an execution and returns hashmap having raw dataset string as
   * key and corresponding resolved dataset string as value
   *
   * @param execId executionId to be used for querying the database for output datasets
   * @return hashmap with raw dataset string as key and corresponding resolved dataset string as
   * value
   * @throws ExecutorManagerException
   */
  public Map<String, String> fetchAllOutputDatasets(final int execId)
      throws ExecutorManagerException {
    try {
      final Map<String, String> datasetList = this.dbOperator
          .query(FetchJobDatasetHandler.FETCH_ALL_OUTPUT_DATASETS, new FetchJobDatasetHandler(),
              execId, "output");
      return datasetList;
    } catch (final SQLException e) {
      final String errorMsg = String
          .format("Error fetching output datasets for execId: %s", execId);
      throw new ExecutorManagerException(errorMsg, e);
    }
  }

  private static class FetchJobDatasetHandler implements ResultSetHandler<Map<String, String>> {
    private static final String FETCH_ALL_OUTPUT_DATASETS =
        "SELECT raw_dataset, resolved_dataset FROM execution_job_dataset WHERE exec_id=? AND dataset_type=?";

    @Override
    public Map<String, String> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyMap();
      }

      final Map<String, String> datasetPathMap = new HashMap<>();
      do {
        final String rawDatasetString = rs.getString(1);
        final String resolvedDatasetString = rs.getString(2);
        final Dataset rawDataset = DatasetUtils.datasetJsonStringToObject(rawDatasetString);
        final Dataset resolvedDataset = DatasetUtils.datasetJsonStringToObject(resolvedDatasetString);
        datasetPathMap.put(rawDataset.getPath(), resolvedDataset.getPath());
      } while (rs.next());

      return datasetPathMap;
    }
  }

  /**
   * Persists dataset object in the database for the given execution
   * @param jobDatasetList List of objects to be persisted
   * @param execId executionId for the dataset objects to be persisted
   * @throws ExecutorManagerException
   */
  public void persistDatasetForExecId(final List<ExecutionJobDataset> jobDatasetList, final int execId)
      throws ExecutorManagerException {

    String sqlCommand = "";

    try {
      Object[][] parameters = jobDatasetList.stream()
          .map(jobDataset -> {
            ArrayList<Object> object = new ArrayList<>();
            object.add(execId);
            object.add(jobDataset.getJobId());
            object.add(jobDataset.getAttempt());
            object.add(jobDataset.getDatasetType());
            object.add(jobDataset.getRawDataset());
            object.add(jobDataset.getResolvedDataset());
            return object.toArray();
          })
          .collect(Collectors.toList()).toArray(new Object[0][]);

      if (parameters.length > 0) {
        sqlCommand = "INSERT INTO execution_job_dataset (exec_id, job_id, attempt, dataset_type, "
            + "raw_dataset, resolved_dataset) VALUES(?,?,?,?,?,?)";

        this.dbOperator.batch(sqlCommand, parameters);
      }
    } catch (final SQLException e) {
      throw new ExecutorManagerException(String.format(
          "Error on inserting into execution_job_dataset, by command [%s]", sqlCommand), e);
    }
  }

  /**
   * Fetches all datasets(input & output) for an executionId
   * @param execId
   * @return List of dataset objects
   * @throws ExecutorManagerException
   */
  public List<ExecutionJobDataset> fetchAllDatasets(final int execId) throws ExecutorManagerException {
    try {
      List<ExecutionJobDataset> jobDatasetList = this.dbOperator
          .query(FetchDatasetEntityHandler.FETCH_ALL_DATASETS, new FetchDatasetEntityHandler(), execId);
      return jobDatasetList;
    } catch (final SQLException e) {
      String errorMsg = String.format("Error fetching all datasets for execId: %s", execId);
      throw new ExecutorManagerException(errorMsg, e);
    }
  }

  private static class FetchDatasetEntityHandler implements ResultSetHandler<List<ExecutionJobDataset>> {
    private static final String FETCH_ALL_DATASETS =
        "SELECT job_id, attempt, dataset_type, raw_dataset, resolved_dataset "
            + "FROM execution_job_dataset where exec_id = ?";

    @Override
    public List<ExecutionJobDataset> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      List<ExecutionJobDataset> jobDatasetList = new ArrayList<>();
      do {
        final String jobId = rs.getString(1);
        final int attempt = rs.getInt(2);
        final String datasetType = rs.getString(3);
        final String rawDataset = rs.getString(4);
        final String resolvedDataset = rs.getString(5);
        ExecutionJobDataset jobDataset = new ExecutionJobDataset(jobId, attempt, datasetType, rawDataset, resolvedDataset);
        jobDatasetList.add(jobDataset);
      } while (rs.next());

      return jobDatasetList;
    }
  }

}
