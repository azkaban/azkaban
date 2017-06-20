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

package azkaban;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;


/**
 * The {@link ServiceProvider} class is an interface to fetch any external dependency. Under the
 * hood it simply maintains a Guice {@link Injector} which is used to fetch the required service
 * type. The current direction of utilization of Guice is to gradually move classes into the Guice
 * scope so that Guice can automatically resolve dependencies and provide the required services
 * directly.
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum ServiceProvider {
  SERVICE_PROVIDER;

  private Injector injector = null;

  /**
   * Ensure that injector is set only once!
   *
   * @param injector Guice injector is itself used for providing services.
   */
  public synchronized void setInjector(final Injector injector) {
    checkState(this.injector == null, "Injector is already set");
    this.injector = requireNonNull(injector, "arg injector is null");
  }

  public synchronized void unsetInjector() {
    this.injector = null;
  }

  public <T> T getInstance(final Class<T> clazz) {
    return requireNonNull(this.injector).getInstance(clazz);
  }

}
