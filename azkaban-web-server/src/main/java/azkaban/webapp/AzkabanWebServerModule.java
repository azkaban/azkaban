/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.webapp;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.event.EventListener;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutionController;
import azkaban.executor.ExecutorHealthChecker;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.OnContainerizedExecutionEventListener;
import azkaban.executor.OnExecutionEventListener;
import azkaban.executor.container.ContainerCleanupManager;
import azkaban.executor.container.ContainerizedWatch;
import azkaban.executor.FlowStatusChangeEventListener;
import azkaban.executor.container.watch.AzPodStatusDrivingListener;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.ContainerizedImplType;
import azkaban.executor.container.watch.AzPodStatusListener;
import azkaban.executor.container.watch.ContainerStatusMetricsListener;
import azkaban.executor.container.watch.FlowStatusManagerListener;
import azkaban.executor.container.watch.KubernetesWatch;
import azkaban.executor.container.watch.KubernetesWatch.PodWatchParams;
import azkaban.executor.container.watch.RawPodWatchEventListener;
import azkaban.executor.container.watch.WatchUtils;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.database.JdbcFlowTriggerInstanceLoaderImpl;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginException;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
import azkaban.imagemgmt.permission.PermissionManager;
import azkaban.imagemgmt.permission.PermissionManagerImpl;
import azkaban.imagemgmt.services.ImageMgmtCommonService;
import azkaban.imagemgmt.services.ImageMgmtCommonServiceImpl;
import azkaban.imagemgmt.services.ImageRampRuleService;
import azkaban.imagemgmt.services.ImageRampRuleServiceImpl;
import azkaban.imagemgmt.services.ImageRampupService;
import azkaban.imagemgmt.services.ImageRampupServiceImpl;
import azkaban.imagemgmt.services.ImageTypeService;
import azkaban.imagemgmt.services.ImageTypeServiceImpl;
import azkaban.imagemgmt.services.ImageVersionService;
import azkaban.imagemgmt.services.ImageVersionServiceImpl;
import azkaban.imagemgmt.services.ImageVersionMetadataService;
import azkaban.imagemgmt.services.ImageVersionMetadataServiceImpl;
import azkaban.imagemgmt.version.JdbcVersionSetLoader;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.metrics.ContainerizationMetrics;
import azkaban.metrics.ContainerizationMetricsImpl;
import azkaban.metrics.DummyContainerizationMetricsImpl;
import azkaban.project.ProjectManager;
import azkaban.scheduler.ScheduleLoader;
import azkaban.scheduler.TriggerBasedScheduleLoader;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.Props;
import azkaban.webapp.metrics.DummyWebMetricsImpl;
import azkaban.webapp.metrics.WebMetrics;
import azkaban.webapp.metrics.WebMetricsImpl;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.util.Providers;
import io.kubernetes.client.openapi.ApiClient;
import java.lang.reflect.Constructor;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.JarResourceLoader;
import org.mortbay.jetty.Server;

/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanWebServerModule extends AbstractModule {

  private static final Logger log = Logger.getLogger(AzkabanWebServerModule.class);
  private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
  private static final String VELOCITY_DEV_MODE_PARAM = "velocity.dev.mode";
  public static final String FLOW_POD_MONITOR = "FlowPodMonitor";
  private static final String CONTAINER_STATUS_METRICS_HANDLER = "ContainerStatusMetricsHandler";
  private final Props props;

  public AzkabanWebServerModule(final Props props) {
    this.props = props;
  }

  @Provides
  @Singleton
  public FlowTriggerDependencyPluginManager getDependencyPluginManager(final Props props)
      throws FlowTriggerDependencyPluginException {
    //todo chengren311: disable requireNonNull for now in beta since dependency plugin dir is not
    // required. Add it back when flow trigger feature is enabled in production
    String dependencyPluginDir;
    try {
      dependencyPluginDir = props.getString(ConfigurationKeys.DEPENDENCY_PLUGIN_DIR);
    } catch (final Exception ex) {
      dependencyPluginDir = null;
    }
    return new FlowTriggerDependencyPluginManager(dependencyPluginDir);
  }

  @Override
  protected void configure() {
    bind(Server.class).toProvider(WebServerProvider.class);
    bind(ScheduleLoader.class).to(TriggerBasedScheduleLoader.class);
    bind(FlowTriggerInstanceLoader.class).to(JdbcFlowTriggerInstanceLoaderImpl.class);
    bind(ExecutorManagerAdapter.class).to(resolveExecutorManagerAdaptorClassType());
    bind(WebMetrics.class).to(resolveWebMetricsClass()).in(Scopes.SINGLETON);
    bind(EventListener.class).to(resolveEventListenerClass()).in(Scopes.SINGLETON);
    // Implement container metrics based on dispatch method
    bind(ContainerizationMetrics.class).to(resolveContainerMetricsClass()).in(Scopes.SINGLETON);

    // Following bindings will be present if and only if containerized dispatch is enabled.
    bindImageManagementDependencies();
    bindContainerWatchDependencies();
    bindContainerCleanupManager();
    bindOnExecutionEventListener();
    // Workaround to support the transition from bare metal executions using the POLL dispatch
    // model to containerized executions. In that mixed environment cleanup logics to handle stuck
    // executions for both CONTAINERIZED and POLL dispatch models are needed.
    bindExecutorHealthCheckerForContainerization();
  }

  private Class<? extends ContainerizationMetrics> resolveContainerMetricsClass() {
    return isContainerizedDispatchMethodEnabled() ? ContainerizationMetricsImpl.class :
        DummyContainerizationMetricsImpl.class;
  }

  private Class<? extends EventListener> resolveEventListenerClass() {
    return FlowStatusChangeEventListener.class;
  }

  private Class<? extends ContainerizedImpl> resolveContainerizedImpl() {
    final String containerizedImplProperty =
        props.getString(ContainerizedDispatchManagerProperties.CONTAINERIZED_IMPL_TYPE,
            ContainerizedImplType.KUBERNETES.name())
            .toUpperCase();
    return ContainerizedImplType.valueOf(containerizedImplProperty).getImplClass();
  }

  private Class<? extends ExecutorManagerAdapter> resolveExecutorManagerAdaptorClassType() {
    switch (DispatchMethod.getDispatchMethod(this.props
        .getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
            DispatchMethod.PUSH.name()))) {
      case POLL:
        return ExecutionController.class;
      case CONTAINERIZED:
        bind(ContainerizedImpl.class).to(resolveContainerizedImpl());
        bind(ContainerizedWatch.class).to(KubernetesWatch.class);
        return ContainerizedDispatchManager.class;
      case PUSH:
      default:
        return ExecutorManager.class;
    }
  }

  private Class<? extends WebMetrics> resolveWebMetricsClass() {
    return this.props.getBoolean(ConfigurationKeys.IS_METRICS_ENABLED, false) ? WebMetricsImpl.class
        : DummyWebMetricsImpl.class;
  }

  private void bindImageManagementDependencies() {
    if(isContainerizedDispatchMethodEnabled()) {
      bind(ImageTypeService.class).to(ImageTypeServiceImpl.class).in(Scopes.SINGLETON);
      bind(ImageVersionService.class).to(ImageVersionServiceImpl.class).in(Scopes.SINGLETON);
      bind(ImageRampupService.class).to(ImageRampupServiceImpl.class).in(Scopes.SINGLETON);
      bind(VersionSetLoader.class).to(JdbcVersionSetLoader.class).in(Scopes.SINGLETON);
      bind(ImageVersionMetadataService.class).to(ImageVersionMetadataServiceImpl.class).in(Scopes.SINGLETON);
      bind(ImageMgmtCommonService.class).to(ImageMgmtCommonServiceImpl.class).in(Scopes.SINGLETON);
      bind(ImageRampRuleService.class).to(ImageRampRuleServiceImpl.class).in(Scopes.SINGLETON);
      bind(PermissionManager.class).to(PermissionManagerImpl.class).in(Scopes.SINGLETON);
    }
  }

  private boolean isContainerizedDispatchMethodEnabled() {
    return DispatchMethod.isContainerizedMethodEnabled(props
        .getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
            DispatchMethod.PUSH.name()));
  }

  /**
   *  Binds container watch dependencies based on the dispatch method.
   *
   *  Hack Alert: This binding to a 'null' provider in the if-condition is hacky.
   *  This was a required to satisfy the {@code ContainerizedImpl} dependency when the
   *  dispatch method is not containerized. The field is injected in one of the listener
   *  providing methods {@link createFlowPodMonitoringListener}.
   *  The binding is still safe as AzPodStatusListener will never be injected when
   *  containerized dispatch is not enabled.
   *  Fix: The way conditional bindings are currently defined in this module is an anti-pattern in
   *  guice: https://github.com/google/guice/wiki/AvoidConditionalLogicInModules
   *  A more comprehensive fix should split the dispatch-method based bindings into different
   *  modules.
   */
  private void bindContainerWatchDependencies() {
    if(!isContainerizedDispatchMethodEnabled()) {
      bind(ContainerizedImpl.class).toProvider(Providers.of(null));
      return;
    }
    log.info("Binding kubernetes watch dependencies");
    bind(KubernetesWatch.class).in(Scopes.SINGLETON);
  }

  private void bindContainerCleanupManager() {
    if(!isContainerizedDispatchMethodEnabled()) {
      bind(ContainerCleanupManager.class).toProvider(Providers.of(null));
      return;
    }
    log.info("Binding ContainerCleanupManager");
    bind(ContainerCleanupManager.class).in(Scopes.SINGLETON);
  }

  private void bindExecutorHealthCheckerForContainerization() {
    // {@link azkaban.executor.ExecutorHealthChecker} binding should only happen if
    // {@link azkaban.DispatchMethod.CONTAINERIZED} dispatch model is enabled and below properties are defined.
    if(isContainerizedDispatchMethodEnabled()) {
      if(!this.props.getString(
          ConfigurationKeys.AZKABAN_EXECUTOR_HEALTHCHECK_INTERVAL_MIN, "").isEmpty() &&
          !this.props.getString(
              ConfigurationKeys.AZKABAN_EXECUTOR_MAX_FAILURE_COUNT, "").isEmpty()) {
        log.info("Binding ExecutorHealthChecker");
        bind(ExecutorHealthChecker.class).in(Scopes.SINGLETON);
      } else {
        bind(ExecutorHealthChecker.class).toProvider(Providers.of(null));
      }
    }
  }

  private void bindOnExecutionEventListener() {
    if(!isContainerizedDispatchMethodEnabled()) {
      bind(OnExecutionEventListener.class).toProvider(Providers.of(null));
      return;
    }
    log.info("Binding OnExecutionEventListener");
    bind(OnExecutionEventListener.class).to(OnContainerizedExecutionEventListener.class).in(Scopes.SINGLETON);
  }

  @Inject
  @Singleton
  @Provides
  private RawPodWatchEventListener createStatusDrivingListener(final Props azkProps,
      @Named(FLOW_POD_MONITOR) AzPodStatusListener flowPodMonitorListener,
      @Named(CONTAINER_STATUS_METRICS_HANDLER) AzPodStatusListener containerStatusMetricsHandlerListener) {
    AzPodStatusDrivingListener listener = new AzPodStatusDrivingListener(azkProps);
    listener.registerAzPodStatusListener(flowPodMonitorListener);
    listener.registerAzPodStatusListener(containerStatusMetricsHandlerListener);
    return listener;
  }

  @Inject
  @Named(FLOW_POD_MONITOR)
  @Singleton
  @Provides
  private AzPodStatusListener createFlowPodMonitoringListener(
      final Props azkProps,
      final ProjectManager projectManager,
      final ContainerizedImpl containerizedImpl,
      final ExecutorLoader executorLoader,
      final AlerterHolder alerterHolder, final ContainerizationMetrics containerizationMetrics,
      final EventListener eventListener) {
    return new FlowStatusManagerListener(azkProps, projectManager, containerizedImpl,
        executorLoader, alerterHolder,
        containerizationMetrics, eventListener);
  }

  @Inject
  @Named(CONTAINER_STATUS_METRICS_HANDLER)
  @Singleton
  @Provides
  private AzPodStatusListener createContainerStatusMetricsHandlerListener(
      ContainerizationMetrics containerizationMetrics) {
    return new ContainerStatusMetricsListener(containerizationMetrics);
  }

  @Inject
  @Singleton
  @Provides
  private ApiClient createContainerApiClient(final Props azkProps) {
    return WatchUtils.createApiClient(azkProps);
  }

  @Inject
  @Singleton
  @Provides
  private PodWatchParams createPodWatchParams(final Props azkProps) {
    return WatchUtils.createPodWatchParams(azkProps);
  }

  @Inject
  @Singleton
  @Provides
  public UserManager createUserManager(final Props props) {
    final Class<?> userManagerClass = props.getClass(USER_MANAGER_CLASS_PARAM, null);
    final UserManager manager;
    if (userManagerClass != null && userManagerClass.getConstructors().length > 0) {
      log.info("Loading user manager class " + userManagerClass.getName());
      try {
        final Constructor<?> userManagerConstructor = userManagerClass.getConstructor(Props.class);
        manager = (UserManager) userManagerConstructor.newInstance(props);
      } catch (final Exception e) {
        log.error("Could not instantiate UserManager " + userManagerClass.getName());
        throw new RuntimeException(e);
      }
    } else {
      manager = new XmlUserManager(props);
    }
    return manager;
  }

  @Inject
  @Singleton
  @Provides
  public VelocityEngine createVelocityEngine(final Props props) {
    final boolean devMode = props.getBoolean(VELOCITY_DEV_MODE_PARAM, false);

    final VelocityEngine engine = new VelocityEngine();
    engine.setProperty("resource.loader", "classpath, jar");
    engine.setProperty("classpath.resource.loader.class",
        ClasspathResourceLoader.class.getName());
    engine.setProperty("classpath.resource.loader.cache", !devMode);
    engine.setProperty("classpath.resource.loader.modificationCheckInterval",
        5L);
    engine.setProperty("jar.resource.loader.class",
        JarResourceLoader.class.getName());
    engine.setProperty("jar.resource.loader.cache", !devMode);
    engine.setProperty("resource.manager.logwhenfound", false);
    engine.setProperty("input.encoding", "UTF-8");
    engine.setProperty("output.encoding", "UTF-8");
    engine.setProperty("directive.set.null.allowed", true);
    engine.setProperty("resource.manager.logwhenfound", false);
    engine.setProperty("velocimacro.permissions.allow.inline", true);
    engine.setProperty("velocimacro.library.autoreload", devMode);
    engine.setProperty("velocimacro.library",
        "/azkaban/webapp/servlet/velocity/macros.vm");
    engine.setProperty(
        "velocimacro.permissions.allow.inline.to.replace.global", true);
    engine.setProperty("velocimacro.arguments.strict", true);
    engine.setProperty("runtime.log.invalid.references", devMode);
    engine.setProperty("runtime.log.logsystem.class", Log4JLogChute.class);
    engine.setProperty("runtime.log.logsystem.log4j.logger",
        Logger.getLogger("org.apache.velocity.Logger"));
    engine.setProperty("parser.pool.size", 3);
    return engine;
  }
}
