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
import azkaban.executor.ExecutionController;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.executor.container.ContainerizedDispatchManager;
import azkaban.executor.container.ContainerizedImpl;
import azkaban.executor.container.ContainerizedImplType;
import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import azkaban.flowtrigger.database.JdbcFlowTriggerInstanceLoaderImpl;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginException;
import azkaban.flowtrigger.plugin.FlowTriggerDependencyPluginManager;
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
import java.lang.reflect.Constructor;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.JarResourceLoader;
import org.eclipse.jetty.server.Server;

/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanWebServerModule extends AbstractModule {

  private static final Logger log = Logger.getLogger(AzkabanWebServerModule.class);
  private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
  private static final String VELOCITY_DEV_MODE_PARAM = "velocity.dev.mode";
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
