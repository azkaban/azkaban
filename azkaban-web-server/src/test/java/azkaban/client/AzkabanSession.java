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

public class AzkabanSession {

	private final String sessionId;
	private final String status;

	@JsonCreator
	public AzkabanSession(@JsonProperty("session.id") String sessionId,
												@JsonProperty("status") String status) {
		this.sessionId = sessionId;
		this.status = status;
	}

	public String getSessionId() {
		return sessionId;
	}

	public String getStatus() {
		return status;
	}

	@Override
	public String toString() {
		return "AzkabanSession{" +
				"sessionId='" + sessionId + '\'' +
				", status='" + status + '\'' +
				'}';
	}
}
