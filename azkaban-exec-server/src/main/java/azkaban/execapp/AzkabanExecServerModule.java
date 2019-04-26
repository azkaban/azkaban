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
package azkaban.execapp;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_ENABLED;

import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanExecServerModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(AzkabanExecServerModule.class);

  @Override
  protected void configure() {
    install(new ExecJettyServerModule());
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class);
  }

  @Inject
  @Provides
  @Singleton
  public AzkabanEventReporter createAzkabanEventReporter(final Props props) {
    final boolean eventReporterEnabled =
        props.getBoolean(AZKABAN_EVENT_REPORTING_ENABLED, false);

    if (!eventReporterEnabled) {
      LOG.info("Event reporter is not enabled");
      return null;
    }

    final Class<?> eventReporterClass =
        props.getClass(AZKABAN_EVENT_REPORTING_CLASS_PARAM, null);
    if (eventReporterClass != null && eventReporterClass.getConstructors().length > 0) {
      LOG.info("Loading event reporter class " + eventReporterClass.getName());
      try {
        final Constructor<?> eventReporterClassConstructor =
            eventReporterClass.getConstructor(Props.class);
        return (AzkabanEventReporter) eventReporterClassConstructor.newInstance(props);
      } catch (final InvocationTargetException e) {
        LOG.error(e.getTargetException().getMessage());
        if (e.getTargetException() instanceof IllegalArgumentException) {
          throw new IllegalArgumentException(e);
        } else {
          throw new RuntimeException(e);
        }
      } catch (final Exception e) {
        LOG.error("Could not instantiate EventReporter " + eventReporterClass.getName());
        throw new RuntimeException(e);
      }
    }
    return null;
  }

}
