package cloudflow.servlets;

import azkaban.server.session.Session;
import azkaban.webapp.AzkabanWebServer;
import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import cloudflow.services.FlowService;
import com.linkedin.jersey.api.uri.UriTemplate;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowServlet extends LoginAbstractAzkabanServlet {

  /* Flow endpoints */
  private static final String ALL_FLOWS_URI = "/flows";
  private static final String FLOW_ID_KEY = "flowId";
  private static final UriTemplate GET_FLOW_URI_TEMPLATE = new UriTemplate(
      String.format("/flows/{%s}", FLOW_ID_KEY));
  private static final Logger log = LoggerFactory.getLogger(FlowServlet.class);
  private FlowService flowService;
  private ObjectMapper objectMapper;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    final AzkabanWebServer server = (AzkabanWebServer) getApplication();
    this.flowService = server.flowService();
    this.objectMapper = server.objectMapper();
  }

  private void validateId(String id, HttpServletResponse resp) throws IOException {
    /* Validate id is an Integer. */
    try {
      Integer.parseInt(id);
    } catch (NumberFormatException e) {
      log.error("Invalid id: ", id);
      sendErrorResponse(resp, HttpServletResponse.SC_BAD_REQUEST, "Invalid id, id must be an "
          + "integer");
      return;
    }
  }

  private void returnAllFlows(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException {
    sendResponse(resp, HttpServletResponse.SC_OK,
        flowService.getAllFlows(session.getUser(), req.getParameterMap()));
  }

  private void returnFlow(HttpServletRequest req, HttpServletResponse resp, Session session,
      Map<String, String> templateVariableToValue) throws IOException {
    try {
      String flowId = templateVariableToValue.get(FLOW_ID_KEY);
      validateId(flowId, resp);
      sendResponse(resp, HttpServletResponse.SC_OK,
          flowService.getFlow(flowId, req.getParameterMap()));
    } catch (Exception e) {
      log.error("Exception while fetching flow: " + e);
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_FOUND, "Flow id not found");
      return;
    }
  }

  @Override
  protected void handleGet(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws IOException, ServletException {
    Map<String, String> templateVariableToValue = new HashMap<>();
    if (ALL_FLOWS_URI.equals(req.getRequestURI())) {
      returnAllFlows(req, resp, session);
    } else if (GET_FLOW_URI_TEMPLATE.match(req.getRequestURI(), templateVariableToValue)) {
      returnFlow(req, resp, session, templateVariableToValue);
    } else {
      /* Unsupported route, return an error */
      log.error("Invalid route for flows endpoint: " + req.getRequestURI());
      sendErrorResponse(resp, HttpServletResponse.SC_NOT_IMPLEMENTED, "Unsupported flows API "
          + "endpoint");
      return;
    }
  }

  @Override
  protected void handlePost(HttpServletRequest req, HttpServletResponse resp, Session session)
      throws ServletException, IOException {
    sendErrorResponse(resp, HttpServletResponse.SC_NOT_IMPLEMENTED, "Unsupported flows API "
        + "endpoint");
  }

}
