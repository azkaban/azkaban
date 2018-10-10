package azkaban.executor;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import org.apache.log4j.Logger;

public class ActiveExecutors {

  private static final Logger logger = Logger.getLogger(ExecutorManager.class);

  private volatile ImmutableSet<Executor> activeExecutors;
  private final Props azkProps;
  private final ExecutorLoader executorLoader;

  @Inject
  public ActiveExecutors(final Props azkProps, final ExecutorLoader executorLoader) {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
  }

  public void setupExecutors() throws ExecutorManagerException {
    final Set<Executor> newExecutors = new HashSet<>();

    if (ExecutorManager.isMultiExecutorMode(this.azkProps)) {
      logger.info("Initializing multi executors from database.");
      newExecutors.addAll(this.executorLoader.fetchActiveExecutors());
    } else if (this.azkProps.containsKey(ConfigurationKeys.EXECUTOR_PORT)) {
      // add local executor, if specified as per properties
      final String executorHost = this.azkProps
          .getString(Constants.ConfigurationKeys.EXECUTOR_HOST, "localhost");
      final int executorPort = this.azkProps.getInt(ConfigurationKeys.EXECUTOR_PORT);
      logger.info(String.format("Initializing local executor %s:%d",
          executorHost, executorPort));
      Executor executor =
          this.executorLoader.fetchExecutor(executorHost, executorPort);
      if (executor == null) {
        executor = this.executorLoader.addExecutor(executorHost, executorPort);
      } else if (!executor.isActive()) {
        executor.setActive(true);
        this.executorLoader.updateExecutor(executor);
      }
      newExecutors.add(new Executor(executor.getId(), executorHost,
          executorPort, true));
    } else {
      // throw exception when in single executor mode and no executor port specified in azkaban
      // properties
      //todo chengren311: convert to slf4j and parameterized logging
      final String error = "Missing" + ConfigurationKeys.EXECUTOR_PORT + " in azkaban properties.";
      logger.error(error);
      throw new ExecutorManagerException(error);
    }

    if (newExecutors.isEmpty()) {
      final String error = "No active executor found";
      logger.error(error);
      throw new ExecutorManagerException(error);
    } else {
      this.activeExecutors = ImmutableSet.copyOf(newExecutors);
    }
  }

  public Collection<Executor> getAll() {
    return activeExecutors;
  }

}
