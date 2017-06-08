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
 */

package azkaban.metrics;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;


/**
 * This class is designed for a utility class to test drop wizard metrics
 */
public class MetricsTestUtility {

  private final MetricRegistry registry;

  public MetricsTestUtility(final MetricRegistry registry) {
    this.registry = registry;
  }

  public static void initServiceProvider() {
    final Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsManager.class).in(Scopes.SINGLETON);
      }
    });
    // Because SERVICE_PROVIDER is a singleton and it is shared among many tests,
    // need to reset the state to avoid assertion failures.
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);
  }

  public long getGaugeValue(final String name) {
    // Assume that the gauge value can be converted to type long.
    return (long) this.registry.getGauges().get(name).getValue();
  }
}
