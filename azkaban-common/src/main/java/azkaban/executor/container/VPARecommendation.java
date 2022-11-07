package azkaban.executor.container;

public class VPARecommendation {
  private String cpuRecommendation;
  private String memoryRecommendation;

  public VPARecommendation(
      final String cpuRecommendation,
      final String memoryRecommendation) {
    this.cpuRecommendation = cpuRecommendation;
    this.memoryRecommendation = memoryRecommendation;
  }

  public String getCpuRecommendation() {
    return this.cpuRecommendation;
  }

  public void setCpuRecommendation(final String cpuRecommendation) {
    this.cpuRecommendation = cpuRecommendation;
  }

  public String getMemoryRecommendation() {
    return this.memoryRecommendation;
  }

  public void setMemoryRecommendation(final String memoryRecommendation) {
    this.memoryRecommendation = memoryRecommendation;
  }
}
