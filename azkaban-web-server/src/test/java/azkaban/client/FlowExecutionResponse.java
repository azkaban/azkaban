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

public class FlowExecutionResponse {

    private final String flow;
    private final String execId;
    private final String project;
    private final String message;

    @JsonCreator
    public FlowExecutionResponse(@JsonProperty("flow") String flow,
                                 @JsonProperty("execid") String execId,
                                 @JsonProperty("project") String project,
                                 @JsonProperty("message") String message) {
        this.flow = flow;
        this.execId = execId;
        this.project = project;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getFlow() {
        return flow;
    }

    public String getExecId() {
        return execId;
    }

    public String getProject() {
        return project;
    }

    @Override
    public String toString() {
        return "FlowExecutionResponse{" +
                "flow='" + flow + '\'' +
                ", execId='" + execId + '\'' +
                ", project='" + project + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}