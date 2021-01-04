/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.container.models;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrSubstitutor;

/**
 * A class which creates the {@link V1Service} object based on the template and substituting values.
 * Each instance of the class is unique to a template which will packages in the jar as a resource.
 * After the instantiation, this class object can be reused to create different service objects.
 */
public class AzKubernetesV1ServiceBuilder {

  public static final String EXEC_ID = "exec_id";
  public static final String SERVICE_NAME = "service_name";
  public static final String NAMESPACE = "namespace";
  public static final String API_VERSION = "api_version";
  public static final String KIND = "kind";
  public static final String PORT = "port";
  public static final String TIMEOUT_MS = "timeout_ms";
  private final Builder<Object, Object> templateValuesMapBuilder = ImmutableMap.builder();
  private final String template;

  /**
   * Constructor for instantiation of this class
   *
   * @param templateName name of the resource within the package
   * @throws IOException if the resource file is not readable
   */
  public AzKubernetesV1ServiceBuilder(final String templateName) throws IOException {
    try (final InputStream is = this.getClass().getResourceAsStream(templateName)) {
      this.template = IOUtils.toString(is, Charsets.UTF_8).trim();
    }
  }

  public AzKubernetesV1ServiceBuilder withExecId(final String execId) {
    this.templateValuesMapBuilder.put(EXEC_ID, execId);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withServiceName(final String serviceName) {
    this.templateValuesMapBuilder.put(SERVICE_NAME, serviceName);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withNamespace(final String namespace) {
    this.templateValuesMapBuilder.put(NAMESPACE, namespace);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withApiVersion(final String apiVersion) {
    this.templateValuesMapBuilder.put(API_VERSION, apiVersion);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withKind(final String kind) {
    this.templateValuesMapBuilder.put(KIND, kind);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withPort(final String port) {
    this.templateValuesMapBuilder.put(PORT, port);
    return this;
  }

  public AzKubernetesV1ServiceBuilder withTimeoutMs(final String timeoutMs) {
    this.templateValuesMapBuilder.put(TIMEOUT_MS, timeoutMs);
    return this;
  }

  /**
   * @return {@link V1Service} object by replacing the values from the templateValuesMap within the
   * template
   * @throws IOException if the object can't be constructed from the substituted YAML String
   */
  public V1Service build() throws IOException {
    final ImmutableMap<Object, Object> templateValuesMap = this.templateValuesMapBuilder.build();
    final StrSubstitutor strSubstitutor = new StrSubstitutor(templateValuesMap);
    final String serviceYaml = strSubstitutor.replace(this.template);
    return (V1Service) Yaml.load(serviceYaml);
  }
}
