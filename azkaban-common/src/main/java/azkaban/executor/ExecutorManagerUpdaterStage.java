package azkaban.executor;

public class ExecutorManagerUpdaterStage {

  private String value = "not started";

  public String get() {
    return value;
  }

  public void set(String value) {
    this.value = value;
  }

}
