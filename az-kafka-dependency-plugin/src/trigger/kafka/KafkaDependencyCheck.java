package trigger.kafka;
import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import azkaban.flowtrigger.DependencyInstanceRuntimeProps;
import azkaban.flowtrigger.DependencyPluginConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import trigger.kafka.Constants.DependencyInstanceRuntimeConfigKey;

public class KafkaDependencyCheck implements DependencyCheck {
  private static final Logger log = Logger.getLogger(KafkaDependencyInstanceContext.class);
  private final ExecutorService executorService;
  private KafkaEventMonitor dependencyMonitor;

  public  KafkaDependencyCheck() {
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public boolean remove(final DependencyInstanceContext depContext) {
    final KafkaDependencyInstanceContext depContextCasted = (KafkaDependencyInstanceContext)
        depContext;
    this.dependencyMonitor.remove(depContextCasted);
    return true;
  }

  @Override
  public DependencyInstanceContext run(final DependencyInstanceConfig config,
      final DependencyInstanceRuntimeProps runtimeProps,
      final DependencyInstanceCallback callback){
    final String starttimeStr = runtimeProps.get(DependencyInstanceRuntimeConfigKey.START_TIME);
    final String triggerInstId = runtimeProps
        .get(DependencyInstanceRuntimeConfigKey.TRIGGER_INSTANCE_ID);
    final KafkaDependencyInstanceContext depInstance = new KafkaDependencyInstanceContext
        (config, this, callback, Long.valueOf(starttimeStr), triggerInstId);

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

    this.dependencyMonitor = new KafkaEventMonitor(config);
    this.executorService.submit(this.dependencyMonitor);
  }

}
