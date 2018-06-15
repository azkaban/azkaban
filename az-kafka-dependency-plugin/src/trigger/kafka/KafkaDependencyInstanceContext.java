package trigger.kafka;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;

import com.google.common.collect.ImmutableSet;
import trigger.kafka.Constants.DependencyInstanceConfigKey;
import trigger.kafka.Constants.DependencyInstanceRuntimeConfigKey;
import trigger.kafka.Constants.DependencyPluginConfigKey;
import org.apache.log4j.Logger;

public class KafkaDependencyInstanceContext implements DependencyInstanceContext {
  private static final Logger log = Logger.getLogger(KafkaDependencyInstanceContext.class);
  private final KafkaDependencyCheck depCheck;
  private final DependencyInstanceCallback callback;
  private final String triggerInstId;
  private final String depName;
  private final String topicName;
  private int counter;

  public KafkaDependencyInstanceContext(final DependencyInstanceConfig config,
      final KafkaDependencyCheck dependencyCheck,
      final DependencyInstanceCallback callback, final long startTime, final String triggerInstId) {
    this.topicName = config.get(DependencyInstanceConfigKey.TOPIC);
    this.callback = callback;
    this.depCheck = dependencyCheck;
    this.triggerInstId = triggerInstId;
    this.depName = config.get(DependencyInstanceConfigKey.EVENT);
    this.counter = Integer.parseInt(config.get(DependencyInstanceConfigKey.COUNTER));
  }
  @Override
  public void cancel() {
    log.info(
        String.format("ready to cancel dependency %s, performing last dependency check", this));
    final boolean shouldCancel = this.depCheck.remove(this);
    if (!shouldCancel) {
      log.info(String.format("Dependency %s becomes available just before being cancelled, sending "
          + "success callback", this));
      this.callback.onSuccess(this);
    } else {
      log.info(String.format("Dependency %s is still not available, sending cancel callback",
          this));
      this.callback.onCancel(this);
    }
  }
  public String getDepName() {
    return this.depName;
  }
  public String getTopicName() {
    return this.topicName;
  }
  public DependencyInstanceCallback getCallback() {
    return this.callback;
  }
  public int eventCaptured(){
    this.counter-=1;
    return counter;
  }
}
