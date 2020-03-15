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

public class UnscheduleCronResponse {

	private final String message;
	private final String status;

	@JsonCreator
	public UnscheduleCronResponse(@JsonProperty("message") String message,
																@JsonProperty("status") String status) {
		this.message = message;
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public String getStatus() {
		return status;
	}

	@Override
	public String toString() {
		return "UnscheduleCronResponse{" +
				"message='" + message + '\'' +
				", status='" + status + '\'' +
				'}';
	}
}