package cloudflow.servlets;

import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.error.CloudFlowException;
import cloudflow.models.JobExecution;
import cloudflow.services.ExecutionService;
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

    private static final Pattern GET_JOB_EXECUTION_INFO_PATTERN = Pattern.compile(
        "/executions/([0-9]+)/jobs/([0-9]+)/([a-zA-Z1-9:_-]+)"); // TODO: remove "/[a-zA-Z1-9:_-]+"

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

        Matcher matcher = GET_JOB_EXECUTION_INFO_PATTERN.matcher(req.getRequestURI());
        if(matcher.matches()) {
            String executionId = matcher.group(1);
            String jobDefinitionId = matcher.group(3); // TODO: replace with group 2
            logger.info("Getting execution details of job {} in execution {}.", jobDefinitionId,
                executionId);
            Optional<JobExecution> jobExecution;
            try {
                jobExecution = executorService.getJobExecution(executionId, jobDefinitionId,
                    session.getUser().getUserId());
            } catch (CloudFlowException e) {
                sendErrorResponse(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    DEFAULT_500_ERROR_MESSAGE);
                return;
            }

            if(jobExecution.isPresent()) {
                sendResponse(resp, HttpServletResponse.SC_OK, toJsonMap(jobExecution.get()));
            } else {
                sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND,
                    String.format("Execution %s not found or doesn't have job with id %s.",
                        executionId, jobDefinitionId));
            }
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
