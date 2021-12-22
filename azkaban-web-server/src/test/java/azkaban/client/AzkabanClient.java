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

import org.apache.http.HttpStatus;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class AzkabanClient {

	private AzkabanSession session;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final String url;
	private final CloseableHttpClient httpClient;

	public AzkabanClient(String url,
						 String username,
						 String password,
						 int retries) throws Exception {

		this.url = url;

		HttpPost post = new HttpPost(url);

		List<NameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair("username", username));
		params.add(new BasicNameValuePair("password", password));
		params.add(new BasicNameValuePair("action", "login"));
		post.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8));

		post.addHeader("accept", "application/json");

		httpClient = HttpClients.createDefault();

		int numRetries = 0;
		System.out.print("getting session");
		while (numRetries < retries) {
			System.out.print(".");
			try (CloseableHttpResponse response = httpClient.execute(post)) {
				String result = EntityUtils.toString(response.getEntity());
				AzkabanSession session = objectMapper.readValue(result, AzkabanSession.class);
				if (!session.getStatus().equals("success")) {
					Thread.sleep(500);
					continue;
				}
				this.session = session;
				System.out.print("\n");
				break;
			} catch (Exception e) {
				numRetries++;
				Thread.sleep(500);
			}
		}
	}

	public void deleteProject(String name) throws IOException, URISyntaxException {
		modifyProject(name, "delete");
	}

	private <T> T checkAndReturn(HttpUriRequest r, Class<T> clazz) throws IOException {
		try (CloseableHttpResponse response = httpClient.execute(r)) {
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new RuntimeException("invalid HTTP status code");
			}
			String result = EntityUtils.toString(response.getEntity());
			return objectMapper.readValue(result, clazz);
		}
	}

	private HttpPost buildPost(String path) {
		HttpPost post = new HttpPost(url + "/" + path);
		post.addHeader("accept", "application/json");
		return post;
	}

	public UploadProjectResponse uploadProjectZip(String name, File zipFile) throws IOException {

		HttpPost post = buildPost("manager");

		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
		builder.addPart("ajax", new StringBody("upload", ContentType.TEXT_PLAIN));
		builder.addPart("file", new FileBody(zipFile, "application/zip"));
		builder.addPart("session.id", new StringBody(session.getSessionId(), ContentType.TEXT_PLAIN));
		builder.addPart("project", new StringBody(name, ContentType.TEXT_PLAIN));
		HttpEntity entity = builder.build();

		post.setEntity(entity);

		return checkAndReturn(post, UploadProjectResponse.class);

	}

	private List<NameValuePair> buildParams() {
		List<NameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair("session.id", session.getSessionId()));
		return params;
	}

	public CreateProjectResponse createProject(String name, String desc) throws IOException {

		HttpPost post = buildPost("manager");

		List<NameValuePair> params = buildParams();
		params.add(new BasicNameValuePair("action", "create"));
		params.add(new BasicNameValuePair("name", name));
		params.add(new BasicNameValuePair("description", desc));
		post.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8));

		return checkAndReturn(post, CreateProjectResponse.class);

	}

	public ScheduleCronResponse scheduleFlow(String projectName, String flowName, String cronExp) throws IOException {

		HttpPost post = buildPost("schedule");

		List<NameValuePair> params = buildParams();
		params.add(new BasicNameValuePair("ajax", "scheduleCronFlow"));
		params.add(new BasicNameValuePair("projectName", projectName));
		params.add(new BasicNameValuePair("flow", flowName));
		params.add(new BasicNameValuePair("cronExpression", cronExp));
		post.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8));

		return checkAndReturn(post, ScheduleCronResponse.class);

	}

	public UnscheduleCronResponse unscheduleFlow(String scheduleId) throws IOException {

		HttpPost post = buildPost("schedule");

		List<NameValuePair> params = buildParams();
		params.add(new BasicNameValuePair("scheduleId", scheduleId));
		params.add(new BasicNameValuePair("action", "removeSched"));
		post.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8));

		return checkAndReturn(post, UnscheduleCronResponse.class);

	}

	private URIBuilder getBuilder(String path) throws URISyntaxException {
		URIBuilder builder = new URIBuilder(url + "/" + path);
		builder.setParameter("session.id", session.getSessionId());
		return builder;
	}

	private HttpGet buildGet(URIBuilder builder) throws URISyntaxException {
		HttpGet get = new HttpGet(builder.build());
		get.addHeader("accept", "application/json");
		return get;
	}

	public void modifyProject(String name, String action) throws IOException, URISyntaxException {

		URIBuilder builder = getBuilder("manager")
				.setParameter("project", name)
				.setParameter(action, "true");

		try (CloseableHttpResponse response = httpClient.execute(buildGet(builder))) {
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new RuntimeException("invalid HTTP status code");
			}
		}

	}

	public FlowExecutionResponse executeFlow(String projectName, String flowName) throws IOException, URISyntaxException {

		URIBuilder builder = getBuilder("executor")
				.setParameter("ajax", "executeFlow")
				.setParameter("project", projectName)
				.setParameter("flow", flowName);

		return checkAndReturn(buildGet(builder), FlowExecutionResponse.class);

	}

	public FlowExecutions getFlowExecutions(String projectName,
																					String flowName, int length) throws IOException, URISyntaxException {

		URIBuilder builder = getBuilder("manager")
				.setParameter("ajax", "fetchFlowExecutions")
				.setParameter("project", projectName)
				.setParameter("flow", flowName)
				.setParameter("start", "0")
				.setParameter("length", Integer.toString(length));

		return checkAndReturn(buildGet(builder), FlowExecutions.class);

	}

	public void close() throws IOException {
		httpClient.close();
	}

}
