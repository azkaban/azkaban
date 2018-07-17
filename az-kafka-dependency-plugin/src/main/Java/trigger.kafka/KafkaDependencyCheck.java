package trigger.kafka;

import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import azkaban.flowtrigger.DependencyPluginConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import trigger.kafka.Constants.DependencyInstanceConfigKey;
import trigger.kafka.Constants.DependencyInstanceRuntimeConfigKey;
import trigger.kafka.Constants.DependencyPluginConfigKey;


public class KafkaDependencyCheck implements DependencyCheck {
  private static final Logger log = Logger.getLogger(KafkaDependencyInstanceContext.class);
  private final ExecutorService executorService;
  private KafkaEventMonitor dependencyMonitor;

  public KafkaDependencyCheck() {
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public boolean remove(final DependencyInstanceContext depContext) {
    final KafkaDependencyInstanceContext depContextCasted = (KafkaDependencyInstanceContext) depContext;
    this.dependencyMonitor.remove(depContextCasted);
    return true;
  }

  private void validate(final DependencyInstanceConfig config, final DependencyInstanceRuntimeProps runtimeProps) {
    final String startTimeStr = runtimeProps.get(DependencyInstanceRuntimeConfigKey.START_TIME);
    final String triggerInstId = runtimeProps.get(DependencyInstanceRuntimeConfigKey.TRIGGER_INSTANCE_ID);
    Preconditions.checkNotNull(triggerInstId,
        DependencyInstanceRuntimeConfigKey.TRIGGER_INSTANCE_ID + " has to be passed in by Azkaban");
    final String LOG_SUFFIX =
        String.format("for dependency instance[trigger instance id: %s, " + "dependency name: %s]", triggerInstId,
            config.get(DependencyInstanceConfigKey.NAME));

    Preconditions.checkNotNull(startTimeStr,
        DependencyInstanceRuntimeConfigKey.START_TIME + " has to be passed in by Azkaban " + LOG_SUFFIX);
    final String topic = config.get(DependencyInstanceConfigKey.TOPIC);
    final String match = config.get(DependencyInstanceConfigKey.MATCH);
    Preconditions.checkNotNull(topic, DependencyInstanceConfigKey.TOPIC + " cannot be null " + LOG_SUFFIX);
    Preconditions.checkNotNull(match, DependencyInstanceConfigKey.MATCH + " cannot be null " + LOG_SUFFIX);
  }

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps, final DependencyInstanceCallback callback) {
    this.validate(config, runtimeProps);
    final String starttimeStr = runtimeProps.get(DependencyInstanceRuntimeConfigKey.START_TIME);
    final String triggerInstId = runtimeProps.get(DependencyInstanceRuntimeConfigKey.TRIGGER_INSTANCE_ID);
    final KafkaDependencyInstanceContext depInstance =
        new KafkaDependencyInstanceContext(config, this, callback, Long.valueOf(starttimeStr), triggerInstId);

    this.dependencyMonitor.add(depInstance);
    return depInstance;
  }

  @Override
  public void shutdown() {
    log.info("Shutting down KafkaDependencyCheck");
    // disallow new tasks
    this.executorService.shutdown();

    try {
      // interrupt current threads;
      this.executorService.shutdownNow();
      // Wait a while for tasks to respond to being cancelled
      if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        log.error("KafkaDependencyCheck does not terminate.");
      }
    } catch (final InterruptedException ex) {
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void init(final DependencyPluginConfig config) {
    //need to add an required for dependency check
    final Set<String> required =
        Sets.newHashSet(DependencyPluginConfigKey.KAKFA_BROKER_URL, DependencyPluginConfigKey.SCHEMA_REGISTRY_URL);
    for (final String requiredField : required) {
      Preconditions.checkNotNull(config.get(requiredField), requiredField + " is required");
    }
    this.dependencyMonitor = new KafkaEventMonitor(config);
    this.executorService.submit(this.dependencyMonitor);
  }
}
