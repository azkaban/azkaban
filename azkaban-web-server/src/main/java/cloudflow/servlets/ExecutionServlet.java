package cloudflow.servlets;

import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.error.CloudFlowException;
import cloudflow.error.CloudFlowNotFoundException;
import cloudflow.models.JobExecution;
import cloudflow.services.ExecutionService;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static cloudflow.servlets.Constants.*;

public class ExecutionServlet extends LoginAbstractAzkabanServlet {

    private static final UriTemplate GET_JOB_EXECUTION_INFO_URI = new UriTemplate(
        "/executions/{executionId}/jobs/{jobDefinitionId}/{jobPath}"); // TODO: remove jobPath

    private static final Logger logger = LoggerFactory.getLogger(ExecutionServlet.class);
    private ExecutionService executorService;


    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);
        final AzkabanWebServer server = (AzkabanWebServer) getApplication();
        this.executorService = server.getExecutionService();
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
        throws ServletException, IOException {

        Map<String, String> templateVariableToValue = new HashMap<>();
        if (GET_JOB_EXECUTION_INFO_URI.match(req.getRequestURI(), templateVariableToValue)) {
            final String executionId = templateVariableToValue.get("executionId");
            final String jobDefinitionId = templateVariableToValue.get("jobDefinitionId");
            try {
                Integer.parseInt(executionId);
                Integer.parseInt(jobDefinitionId);
            } catch (NumberFormatException e) {
                sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, "Invalid execution id"
                    + " or job definition id.");
                return;
            }

            logger.info("Getting execution details of job {} in execution {}.", jobDefinitionId,
                executionId);
            JobExecution jobExecution;
            try {
                // TODO: replace jobPath with jobDefinitionId
                jobExecution = executorService.getJobExecution(executionId, templateVariableToValue.get(
                    "jobPath"), session.getUser().getUserId());
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

        // TODO: handle unsupported routes
    }

    private static Map<String, Object> toJsonMap(JobExecution jobExecution) {
        Map<String, Object> map = new HashMap<>();
        map.put(EXECUTION_ID_KEY, jobExecution.getExecutionId());
        map.put(START_TIME_KEY, jobExecution.getStartTime());
        map.put(END_TIME_KEY, jobExecution.getEndTime());
        map.put(STATUS_KEY, jobExecution.getStatus().toString());

        List<Map<String, Object>> attempts = jobExecution.getAttempts().stream().map( a -> {
            Map<String, Object> attempt = new HashMap<>();
            attempt.put(ID_KEY, a.getId());
            attempt.put(START_TIME_KEY, a.getStartTime());
            attempt.put(END_TIME_KEY, a.getEndTime());
            attempt.put(STATUS_KEY, a.getStatus().toString());
            return attempt;
        }).collect(Collectors.toList());
        map.put(ATTEMPTS_KEY,attempts);

        return map;
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
        throws ServletException, IOException {

    }
}
