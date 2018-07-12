package trigger.kafka;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import org.apache.log4j.Logger;
import trigger.kafka.Constants.DependencyInstanceConfigKey;


public class KafkaDependencyInstanceContext implements DependencyInstanceContext {
  private static final Logger log = Logger.getLogger(KafkaDependencyInstanceContext.class);
  private final KafkaDependencyCheck depCheck;
  private final DependencyInstanceCallback callback;
  private final String triggerInstId;
  private final String topicName;
  private final String regexMatch;
  private final String depName;

  public KafkaDependencyInstanceContext(final DependencyInstanceConfig config,
      final KafkaDependencyCheck dependencyCheck, final DependencyInstanceCallback callback, final long startTime,
      final String triggerInstId) {
    this.topicName = config.get(DependencyInstanceConfigKey.TOPIC);
    this.callback = callback;
    this.depCheck = dependencyCheck;
    this.triggerInstId = triggerInstId;
    this.regexMatch = config.get(DependencyInstanceConfigKey.MATCH);
    this.depName = config.get(DependencyInstanceConfigKey.NAME);
  }

  @Override
  public void cancel() {
    log.info(String.format("ready to cancel dependency %s, performing last dependency check", this));
    final boolean shouldCancel = this.depCheck.remove(this);
    if (!shouldCancel) {
      log.info(
          String.format("Dependency %s becomes available just before being cancelled, sending " + "success callback",
              this));
      this.callback.onSuccess(this);
    } else {
      log.info(String.format("Dependency %s is still not available, sending cancel callback", this));
      this.callback.onCancel(this);
    }
  }

  public String getRegexMatch() {
    return this.regexMatch;
  }

  public String getTopicName() {

    return this.topicName;
  }

  public DependencyInstanceCallback getCallback() {

    return this.callback;
  }
}
