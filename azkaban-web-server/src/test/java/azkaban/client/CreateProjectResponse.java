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

public class CreateProjectResponse {

	private final String status;
	private final String path;
	private final String action;

	@JsonCreator
	public CreateProjectResponse(@JsonProperty("status") String status,
															 @JsonProperty("path") String path,
															 @JsonProperty("action") String action) {
		this.status = status;
		this.path = path;
		this.action = action;
	}

	public String getStatus() {
		return status;
	}

	public String getPath() {
		return path;
	}

	public String getAction() {
		return action;
	}

	@Override
	public String toString() {
		return "CreateProjectResponse{" +
				"status='" + status + '\'' +
				", path='" + path + '\'' +
				", action='" + action + '\'' +
				'}';
	}
}