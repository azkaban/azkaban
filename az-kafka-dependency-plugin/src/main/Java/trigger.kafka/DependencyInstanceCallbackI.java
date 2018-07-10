package trigger.kafka;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceContext;


public class DependencyInstanceCallbackI implements DependencyInstanceCallback {


  public DependencyInstanceCallbackI() {}

  @Override
  public void onSuccess(DependencyInstanceContext dependencyInstanceContext) {
    System.out.println("SUCCESS");
  }

  @Override
  public void onCancel(DependencyInstanceContext dependencyInstanceContext) {
    System.out.println("CANCLE");
  }
}