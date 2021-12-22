/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.client;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class FlowExecutions {

    private final int total;
    private final int length;
    private final int from;
    private final String flow;
    private final int projectId;
    private final String project;
    private final List<FlowExecution> executions;

    @JsonCreator
    public FlowExecutions(@JsonProperty("total") int total,
                          @JsonProperty("length") int length,
                          @JsonProperty("from") int from,
                          @JsonProperty("flow") String flow,
                          @JsonProperty("projectId") int projectId,
                          @JsonProperty("project") String project,
                          @JsonProperty("executions") List<FlowExecution> executions) {
        this.total = total;
        this.length = length;
        this.from = from;
        this.flow = flow;
        this.projectId = projectId;
        this.project = project;
        this.executions = executions;
    }

    public int getTotal() {
        return total;
    }

    public int getLength() {
        return length;
    }

    public int getFrom() {
        return from;
    }

    public String getFlow() {
        return flow;
    }

    public int getProjectId() {
        return projectId;
    }

    public String getProject() {
        return project;
    }

    public List<FlowExecution> getExecutions() {
        return executions;
    }

    @Override
    public String toString() {
        return "FlowExecutions{" +
                "total=" + total +
                ", length=" + length +
                ", from=" + from +
                ", flow='" + flow + '\'' +
                ", projectId=" + projectId +
                ", project='" + project + '\'' +
                ", executions=" + executions +
                '}';
    }
}
