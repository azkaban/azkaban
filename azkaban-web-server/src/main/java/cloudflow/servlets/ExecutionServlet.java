package cloudflow.servlets;

import azkaban.server.HttpRequestUtils;
import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.error.CloudFlowException;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.error.CloudFlowNotImplementedException;
import cloudflow.error.CloudFlowValidationException;
import cloudflow.models.ExecutionDetailedResponse;
import cloudflow.models.JobExecution;
import cloudflow.services.ExecutionParameters;
import cloudflow.services.ExecutionService;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.util.List;
import java.util.stream.Collectors;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static cloudflow.servlets.Constants.*;

import static java.lang.String.format;


public class ExecutionServlet extends LoginAbstractAzkabanServlet {

  private static final String EXECUTION_ID_KEY = "executionId";
  private static final String JOB_ID_KEY = "jobDefinitionId";
  private static final String JOB_PATH_KEY = "jobPath";

  private static final UriTemplate GET_ALL_EXECUTIONS_URI = new UriTemplate("/executions");
  private static final UriTemplate GET_SINGLE_EXECUTION_URI = new UriTemplate(
      format("/executions/{%s}", EXECUTION_ID_KEY));
  // TODO remove jobPath
  private static final UriTemplate GET_JOB_EXECUTION_INFO_URI = new UriTemplate(
      format("/executions/{%s}/jobs/{%s}/{%s}", EXECUTION_ID_KEY, JOB_ID_KEY, JOB_PATH_KEY));

  private static final Logger logger = LoggerFactory.getLogger(ExecutionServlet.class);
  private ExecutionService executionService;
  private ObjectMapper objectMapper;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.executionService = server.getExecutionService();
    this.objectMapper = server.objectMapper();
  }

  private void handleAllExecutions(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    sendResponse(resp, HttpServletResponse.SC_OK,
        executionService.getAllExecutions(req.getParameterMap()));
  }

  private void handleSingleExecution(HttpServletRequest req, HttpServletResponse resp, Map<String
      , String> templateVariableMap)
      throws IOException {
    String executionId = templateVariableMap.get(EXECUTION_ID_KEY);
    ExecutionDetailedResponse executionResponse;
    try {
      executionResponse = executionService.getExecution(executionId);
    } catch (CloudFlowNotFoundException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, e.getMessage());
      return;
    }
    sendResponse(resp, HttpServletResponse.SC_OK, executionResponse);
  }

  private void handleExecutionWithJobInfo(HttpServletRequest req, HttpServletResponse resp,
      Session session, Map<String, String> templateVariableToValue) throws IOException {
    final String executionId = templateVariableToValue.get(EXECUTION_ID_KEY);
    final String jobDefinitionId = templateVariableToValue.get(JOB_ID_KEY);
    try {
      Integer.parseInt(executionId);
      Integer.parseInt(jobDefinitionId);
    } catch (NumberFormatException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          "Invalid execution id or job definition id.");
      return;
    }

    logger.info("Getting execution details of job {} in execution {}.",
        jobDefinitionId, executionId);
    JobExecution jobExecution;
    try {
      // TODO: replace jobPath with jobDefinitionId
      jobExecution = executionService.getJobExecution(executionId,
          templateVariableToValue.get(JOB_PATH_KEY), session.getUser().getUserId());
    } catch (CloudFlowNotFoundException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, e.getMessage());
      return;
    } catch (CloudFlowException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          DEFAULT_500_ERROR_MESSAGE);
      return;
    }

    sendResponse(resp, HttpServletResponse.SC_OK, toJsonMap(jobExecution));
    return;
  }

  private static Map<String, Object> toJsonMap(JobExecution jobExecution) {
    Map<String, Object> map = new HashMap<>();
    map.put(EXECUTION_ID_KEY, jobExecution.getExecutionId());
    map.put(START_TIME_KEY, jobExecution.getStartTime());
    map.put(END_TIME_KEY, jobExecution.getEndTime());
    map.put(STATUS_KEY, jobExecution.getStatus().toString());

    List<Map<String, Object>> attempts = jobExecution.getAttempts().stream().map(a -> {
      Map<String, Object> attempt = new HashMap<>();
      attempt.put(ID_KEY, a.getId());
      attempt.put(START_TIME_KEY, a.getStartTime());
      attempt.put(END_TIME_KEY, a.getEndTime());
      attempt.put(STATUS_KEY, a.getStatus().toString());
      return attempt;
    }).collect(Collectors.toList());
    map.put(ATTEMPTS_KEY, attempts);

    return map;
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException {
    Map<String, String> templateVariableMap = new HashMap<>();
    String requestUri = req.getRequestURI();

    if (GET_ALL_EXECUTIONS_URI.match(requestUri, templateVariableMap)) {
      handleAllExecutions(req, resp);
    } else if (GET_SINGLE_EXECUTION_URI.match(requestUri, templateVariableMap)) {
      handleSingleExecution(req, resp, templateVariableMap);
    } else if (GET_JOB_EXECUTION_INFO_URI.match(requestUri, templateVariableMap)) {
      handleExecutionWithJobInfo(req, resp, session, templateVariableMap);
    } else {
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, DEFAULT_404_ERROR_MESSAGE);
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException {

    String requestURI = req.getRequestURI();
    if (GET_ALL_EXECUTIONS_URI.match(requestURI, new HashMap<>())) {
      handleCreateExecution(req, resp, session);
    } else {
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, DEFAULT_404_ERROR_MESSAGE);
    }
  }

  private void handleCreateExecution(HttpServletRequest req, HttpServletResponse resp,
      Session session) throws IOException {
    ExecutionParameters executionParameters = extractExecutionParameters(req, resp, session);
    if (executionParameters == null) {
      // this is to stop execution of subsequent code since sendErrorResponse() inside
      // extractExecutionParameters() doesn't conclude execution of code in handleCreateExecution()
      return;
    }

    String executionId;
    try {
      executionId = executionService.createExecution(executionParameters);
    } catch (CloudFlowValidationException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
      return;
    } catch (CloudFlowNotImplementedException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_IMPLEMENTED, e.getMessage());
      return;
    } catch (CloudFlowException e) {
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            DEFAULT_500_ERROR_MESSAGE);
      return;

    }
    logger.info(String.format("New execution of flow %s was successfully queued with id %s",
        executionParameters.getFlowId(), executionId));
    resp.setHeader("Location", GET_SINGLE_EXECUTION_URI.createURI(executionId));
    sendResponse(resp, HttpServletResponse.SC_CREATED, new HashMap<>());
  }

  private ExecutionParameters extractExecutionParameters(final HttpServletRequest req,
    HttpServletResponse resp, Session session) throws IOException {
    ExecutionParameters executionParameters;
    try {
      String body = HttpRequestUtils.getBody(req);
      executionParameters = this.objectMapper.readValue(body, ExecutionParameters.class);
    } catch (JsonMappingException e) {
      final String errorMessage =
          String.format("Parameter '%s' isn't valid.", e.getPath().get(0).getFieldName());
      logger.error(errorMessage + " \n" + e.getMessage());
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, errorMessage);
      return null;
    } catch (JsonParseException e) {
      final String errorMessage = "Malformed JSON content.";
      logger.error(errorMessage + " \n" + e.getMessage());
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, errorMessage);
      return null;
    } catch (Exception e) {
      logger.error(DEFAULT_500_ERROR_MESSAGE, e);
      sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          DEFAULT_500_ERROR_MESSAGE);
      return null;
    }

    if(executionParameters.getFlowId() == null || executionParameters.getFlowId().isEmpty()) {
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          String.format("Parameter '%s' is required.", FLOW_ID_KEY));
      return null;
    }

    if(executionParameters.getFlowVersion() == null) {
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
          String.format("Parameter '%s' is required.", FLOW_VERSION_KEY));
      return null;
    }
    executionParameters.setSubmitUser(session.getUser().getUserId());

    return executionParameters;
  }
}
