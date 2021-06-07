package azkaban.project;

import java.util.HashMap;
import java.util.List;

public class NodeDataset {
  List<HashMap<String, String>> datasets;
  String type;

  public List<HashMap<String, String>> getDatasets() {
    return datasets;
  }

  public void setDatasets(List<HashMap<String, String>> datasets) {
    this.datasets = datasets;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
