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
 *
 */
package azkaban.client;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class FlowExecution {

	private final String startTime;
	private final String submitUser;
	private final String status;
	private final String submitTime;
	private final String execId;
	private final String projectId;
	private final String endTime;
	private final String flowId;

	@JsonCreator
	public FlowExecution(@JsonProperty("startTime") String startTime,
											 @JsonProperty("submitUser") String submitUser,
											 @JsonProperty("status") String status,
											 @JsonProperty("submitTime") String submitTime,
											 @JsonProperty("execId") String execId,
											 @JsonProperty("projectId") String projectId,
											 @JsonProperty("endTime") String endTime,
											 @JsonProperty("flowId") String flowId) {
		this.startTime = startTime;
		this.submitUser = submitUser;
		this.status = status;
		this.submitTime = submitTime;
		this.execId = execId;
		this.projectId = projectId;
		this.endTime = endTime;
		this.flowId = flowId;
	}

	public String getStartTime() {
		return startTime;
	}

	public String getSubmitUser() {
		return submitUser;
	}

	public String getStatus() {
		return status;
	}

	public String getSubmitTime() {
		return submitTime;
	}

	public String getExecId() {
		return execId;
	}

	public String getProjectId() {
		return projectId;
	}

	public String getEndTime() {
		return endTime;
	}

	public String getFlowId() {
		return flowId;
	}

	@Override
	public String toString() {
		return "FlowExecution{" +
				"startTime='" + startTime + '\'' +
				", submitUser='" + submitUser + '\'' +
				", status='" + status + '\'' +
				", submitTime='" + submitTime + '\'' +
				", execId='" + execId + '\'' +
				", projectId='" + projectId + '\'' +
				", endTime='" + endTime + '\'' +
				", flowId='" + flowId + '\'' +
				'}';
	}
}