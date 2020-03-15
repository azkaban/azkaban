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

public class ScheduleCronResponse {

	private final String scheduleId;
	private final String message;
	private final String status;
	private final String projectName;

	@JsonCreator
	public ScheduleCronResponse(@JsonProperty("projectName") String projectName,
															@JsonProperty("scheduleId") String scheduleId,
															@JsonProperty("message") String message,
															@JsonProperty("status") String status) {
		this.projectName = projectName;
		this.scheduleId = scheduleId;
		this.message = message;
		this.status = status;
	}

	public String getProjectName() {
		return projectName;
	}

	public String getScheduleId() {
		return scheduleId;
	}

	public String getMessage() {
		return message;
	}

	public String getStatus() {
		return status;
	}

	@Override
	public String toString() {
		return "CronScheduleResponse{" +
				"scheduleId='" + scheduleId + '\'' +
				", message='" + message + '\'' +
				", status='" + status + '\'' +
				", projectName='" + projectName + '\'' +
				'}';
	}
}