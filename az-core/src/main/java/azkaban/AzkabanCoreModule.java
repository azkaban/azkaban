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

import azkaban.utils.Props;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;


/**
 * The Guice launching place for az-core.
 */
public class AzkabanCoreModule extends AbstractModule {

  private final Props props;

  public AzkabanCoreModule(final Props props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    bind(Props.class).toInstance(this.props);
    bind(MetricRegistry.class).in(Scopes.SINGLETON);
  }
}
