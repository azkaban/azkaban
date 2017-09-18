package azkaban.scheduler;

import java.io.Serializable;
import org.quartz.JobExecutionContext;

public class TestQuartzJob extends AbstractQuartzJob{

  public static final String DELEGATE_CLASS_NAME = "TestService";

  public TestQuartzJob() {
  }

  @Override
  public void execute(final JobExecutionContext context) {
    final TestService service = asT(getKey(context, DELEGATE_CLASS_NAME));
    service.run();
  }
}

class TestService implements Serializable{

  private final String field1;
  private final String field2;

  TestService(final String field1, final String field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  void run() {
    System.out.println("field1: " + this.field1 + "==== field2: " + this.field2);
  }

  @Override
  public String toString() {
    return "field1: " + this.field1 + ", field2: " + this.field2;
  }
}
