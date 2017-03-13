package azkaban.project;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;


public class ProjectSpec implements Serializable {
  private String version;
  private PreExecutionSpec preExec;

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public PreExecutionSpec getPreExec() {
    return preExec;
  }

  public void setPreExec(PreExecutionSpec preExec) {
    this.preExec = preExec;
  }

  @Override
  public String toString() {
    return "ProjectSpec{" + "version='" + version + '\'' + ", preExec=" + preExec + '}';
  }

  public static class PreExecutionSpec implements Serializable {
    private Map<String, URI> fetch;

    public Map<String, URI> getFetch() {
      return fetch;
    }

    public void setFetch(Map<String, URI> fetch) {
      this.fetch = fetch;
    }

    @Override
    public String toString() {
      return "PreExecutionSpec{" + "fetch=" + fetch + '}';
    }
  }

}
