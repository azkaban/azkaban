package azkaban.executor;

public class ExecutionJobDataset {
  String jobId;
  int attempt;
  String datasetType;
  String rawDataset;
  String resolvedDataset;

  public ExecutionJobDataset(String jobId, int attempt, String datasetType, String rawDataset, String resolvedDataset) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.datasetType = datasetType;
    this.rawDataset = rawDataset;
    this.resolvedDataset = resolvedDataset;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public int getAttempt() {
    return attempt;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public String getDatasetType() {
    return datasetType;
  }

  public void setDatasetType(String datasetType) {
    this.datasetType = datasetType;
  }

  public String getRawDataset() {
    return rawDataset;
  }

  public void setRawDataset(String rawDataset) {
    this.rawDataset = rawDataset;
  }

  public String getResolvedDataset() {
    return resolvedDataset;
  }

  public void setResolvedDataset(String resolvedDataset) {
    this.resolvedDataset = resolvedDataset;
  }
}
